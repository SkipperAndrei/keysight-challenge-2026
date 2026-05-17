#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <setjmp.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdbool.h>

#include <rte_common.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_memory.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_cycles.h>
#include <rte_prefetch.h>
#include <rte_lcore.h>
#include <rte_per_lcore.h>
#include <rte_branch_prediction.h>
#include <rte_interrupts.h>
#include <rte_random.h>
#include <rte_debug.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_mempool.h>
#include <rte_mbuf.h>
#include <rte_string_fns.h>

static volatile bool force_quit;

#define RTE_LOGTYPE_NETEM RTE_LOGTYPE_USER1

#define MAX_PKT_BURST 32
#define BURST_TX_DRAIN_US 100 /* TX drain every ~100us */
#define MEMPOOL_CACHE_SIZE 256
#define DELAY_BUF_SIZE 1024

// Convert microseconds to CPU cycles for accurate timing
#define US_TO_CYCLES(X) (((uint64_t)(X)*rte_get_tsc_hz()) / 1000000)

/*
 * Configurable number of RX/TX ring descriptors
 */
#define RX_DESC_DEFAULT 1024
#define TX_DESC_DEFAULT 1024
static uint16_t nb_rxd = RX_DESC_DEFAULT;
static uint16_t nb_txd = TX_DESC_DEFAULT;

/* Number of ports */
#define NB_PORTS 2

#define NUM_QUEUES 10
#define PATTERN_SIZE 12
#define DEFAULT_PQ_INDEX 10

typedef struct delayed_t {
	struct rte_mbuf *m;
	uint64_t send_time;
	uint64_t delay_us;
} delayed_t;

typedef struct packet_t {
	struct rte_mbuf *m;
	uint8_t pq_id;
	uint64_t send_time;
} packet_t;

typedef struct PQ_t {
	unsigned char pattern[PATTERN_SIZE];
	uint8_t pq_id;
	double drop_rate;
	double double_rate;
	uint64_t delay_us;
} PQ_t;


static const PQ_t pq[NUM_QUEUES + 1] = {
    // Pattern, PQ ID, Drop Rate, Duplicate Rate, Delay (us)
    {{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 0, 0, 0, 0},
    {{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 1, 0, 0, 0},
    {{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 2, 0, 0, 0},
	{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 3, 0, 0, 0},
	{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 4, 0, 0, 0},
	{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 5, 0, 0, 0},
    {{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 6, 0, 0, 0},
    {{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 7, 0, 0, 0},
	{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 8, 0, 0, 0},
	{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}, 9, 0, 0, 0},
    
    {{0}, DEFAULT_PQ_INDEX, 0, 0, 0} // Default PQ
};

typedef struct packet_queue_t {
	struct rte_ring *ring;
	struct rte_mempool *item_pool;
} packet_queue_t;

#define QUEUE_RING_SIZE 4096
packet_queue_t *queue;

packet_queue_t *init_packet_queue()
{
	packet_queue_t *queue =
		rte_zmalloc("PQ_STATE", sizeof(struct packet_queue_t), 0);
	char name_buf[] = "ring_pq";

    // Initialize lockless ring optimized for Single-Producer / Single-Consumer mapping
	queue->ring = rte_ring_create(name_buf, QUEUE_RING_SIZE, rte_socket_id(),
								  RING_F_SP_ENQ | RING_F_SC_DEQ);

	// Initialize an ultra-fast local object cache pool for your custom struct elements
	char name_buf[] = "pool_pq";
	queue->item_pool = rte_mempool_create(name_buf, QUEUE_RING_SIZE - 1, sizeof(packet_t),
                                       0, 0, NULL, NULL, NULL, NULL, 
                                       rte_socket_id(), 0);

	if (!queue->ring || !queue->item_pool) {
		rte_exit(EXIT_FAILURE, "Failed to initialize DPDK pipeline rings or pools\n");
	}

	return queue;
}

int enqueue_packet(rte_mbuf *mbuf, PQ_t pq) {
	void *msg = NULL;
	packet_t *pkt;

	if (rte_mempool_get(queue->item_pool, &msg) < 0) {
		// Out of memory structures; drop packet safely
		rte_pktmbuf_free(mbuf);
		return -1;
	}

	pkt = (packet_t *)msg;
	pkt->m = mbuf;
	pkt->pq_id = pq.pq_id;

	if (rte_ring_enqueue(queue->ring, pkt) < 0) {
		// Ring is completely full; clean up allocations to prevent memory leaks
		rte_pktmbuf_free(mbuf);
		rte_mempool_put(queue->item_pool, pkt);
		return -1;
	}

	return 0;
}

int dequeue_packet(packet_t **pkt) {
	void *msg = NULL;

	if (rte_ring_dequeue(queue->ring, &msg) < 0) {
		// Ring is empty; no packet to process
		return -1;
	}

	*pkt = (packet_t *)msg;
	return 0;
}

static inline uint8_t classify_packet_to_config_idx(struct rte_mbuf *m)
{
    uint8_t *pkt_data = rte_pktmbuf_mtod(m, uint8_t *);
    uint32_t pkt_len = rte_pktmbuf_data_len(m);

    if (unlikely(pkt_len < PATTERN_SIZE)) {
        return DEFAULT_PQ_INDEX;
    }

    uint32_t scan_limit = pkt_len - PATTERN_SIZE + 1;

    // Slide across the packet bytes
    for (uint32_t i = 0; i < scan_limit; i++) {
        uint8_t *current_window = &pkt_data[i];

        // Match against your 10 custom patterns
		for (uint8_t p = 0; p < NUM_QUEUES; p++) {
			if (memcmp(current_window, pq[p].pattern, PATTERN_SIZE) == 0) {
				return p; // Return index of the matched configuration profile
			}
		}
	}

    return DEFAULT_PQ_INDEX; // Fallback profile index
}

void worker_process_packet(void *args) {

	uint8_t nb_packets_in_queue = 0;
	PQ_t pq_info;
	while (!force_quit) {
		struct rte_mbuf *pkts_burst[MAX_PKT_BURST];
		nb_packets_in_queue = rte_ring_dequeue_burst(queue->ring, (void **)pkts_burst, MAX_PKT_BURST);

		if (unlikely(nb_packets_in_queue == 0))
			continue;

		for (uint8_t i = 0; i < nb_packets_in_queue; i++) {
			rte_prefetch0(rte_pktmbuf_mtod(pkts_burst[i], void *));
			struct packet_t *m = pkts_burst[i];
			pq_info = pq[m->pq_id];

			if (m->send_time + US_TO_CYCLES(pq_info.delay_us) > rte_rdtsc()) {
				rte_ring_enqueue(queue->ring, m);
				continue;
			}

			if (pq_info.drop_rate > 0 && rte_rand() / (double)UINT32_MAX < pq_info.drop_rate) {
				rte_pktmbuf_free(m);
				continue;
			}

			if (pq_info.double_rate > 0 && rte_rand() / (double)UINT32_MAX < pq_info.double_rate) {
				struct rte_mbuf *dup_pkt = rte_pktmbuf_clone(m, netem_pktmbuf_pool);
				if (dup_pkt != NULL) {
					enqueue_packet(dup_pkt, pq_info);
				} else {
					RTE_LOG(WARNING, NETEM, "Failed to clone packet for doubling in PQ %u\n", pq_info.pq_id);
				}
			}

			// Process the packet (e.g., send it out, or further processing)
		}		
	}
}

/* ethernet addresses of ports */
static struct rte_ether_addr netem_ports_eth_addr[NB_PORTS];

static struct rte_eth_dev_tx_buffer *tx_buffer[NB_PORTS];

static struct rte_eth_conf port_conf = {
	.txmode = {
		.mq_mode = RTE_ETH_MQ_TX_NONE,
	},
};

struct rte_mempool *netem_pktmbuf_pool = NULL;

/* Per-port statistics struct */
struct __rte_cache_aligned netem_port_statistics {
	uint64_t tx;
	uint64_t rx;
	uint64_t dropped;
};
struct netem_port_statistics port_statistics[NB_PORTS];

/* A tsc-based timer responsible for triggering statistics printout */
static uint64_t timer_period = 1; /* default period is 1 seconds */

/* Print out statistics on packets dropped */
static void print_stats(void)
{
	uint64_t total_packets_dropped, total_packets_tx, total_packets_rx;
	unsigned portid;

	total_packets_dropped = 0;
	total_packets_tx = 0;
	total_packets_rx = 0;

	const char clr[] = { 27, '[', '2', 'J', '\0' };
	const char topLeft[] = { 27, '[', '1', ';', '1', 'H', '\0' };

	/* Clear screen and move to top left */
	printf("%s%s", clr, topLeft);

	printf("\nPort statistics ====================================");

	for (portid = 0; portid < NB_PORTS; portid++) {
		printf("\nStatistics for port %u ------------------------------"
			   "\nPackets sent: %24" PRIu64 "\nPackets received: %20" PRIu64
			   "\nPackets dropped: %21" PRIu64,
			   portid, port_statistics[portid].tx, port_statistics[portid].rx,
			   port_statistics[portid].dropped);

		total_packets_dropped += port_statistics[portid].dropped;
		total_packets_tx += port_statistics[portid].tx;
		total_packets_rx += port_statistics[portid].rx;
	}
	printf("\nAggregate statistics ==============================="
		   "\nTotal packets sent: %18" PRIu64
		   "\nTotal packets received: %14" PRIu64
		   "\nTotal packets dropped: %15" PRIu64,
		   total_packets_tx, total_packets_rx, total_packets_dropped);
	printf("\n====================================================\n");

	fflush(stdout);
}

/* main processing loop */
static void netem_main_loop(void)
{
	struct rte_mbuf *pkts_burst[MAX_PKT_BURST];
	struct rte_mbuf *m;
	int sent;
	unsigned lcore_id;
	uint64_t prev_tsc, diff_tsc, cur_tsc, timer_tsc;
	unsigned i, nb_rx;
	const uint64_t drain_tsc =
		(rte_get_tsc_hz() + US_PER_S - 1) / US_PER_S * BURST_TX_DRAIN_US;
	struct rte_eth_dev_tx_buffer *buffer;

	prev_tsc = 0;
	timer_tsc = 0;

	lcore_id = rte_lcore_id();

	/* On the RX path, the lcore reads the packets from it's port.
	 * For lcore_id 0, set the rx_port_id to 0.
	 * For lcore_id 1, set the rx_port_id to 1.
	 */
	uint16_t rx_port_id = lcore_id;

	/* On the TX path, the lcore will send the packets to the other port
	 * For lcore_id 0, set the tx_port_id to 1.
	 * For lcore_id 1, set the tx_port_id to 0.
	 */
	uint16_t tx_port_id = lcore_id ^ 1;

	printf("lcore_id %u, tx %u, rx %u\n", lcore_id, tx_port_id, rx_port_id);

	RTE_LOG(INFO, NETEM, "entering main loop on lcore %u\n", lcore_id);

	while (!force_quit) {
		/* Drains the TX queue after a certain time */
		cur_tsc = rte_rdtsc();

		diff_tsc = cur_tsc - prev_tsc;
		if (unlikely(diff_tsc > drain_tsc)) {
			buffer = tx_buffer[tx_port_id];

			sent = rte_eth_tx_buffer_flush(tx_port_id, 0, buffer);
			if (sent)
				port_statistics[tx_port_id].tx += sent;

			/* if timer is enabled */
			if (timer_period > 0) {
				/* advance the timer */
				timer_tsc += diff_tsc;

				/* if timer has reached its timeout */
				if (unlikely(timer_tsc >= timer_period)) {
					/* do this only on main core */
					if (lcore_id == rte_get_main_lcore()) {
						print_stats();
						/* reset the timer */
						timer_tsc = 0;
					}
				}
			}

			prev_tsc = cur_tsc;
		}

		/* Read packet from RX queue */
		nb_rx = rte_eth_rx_burst(rx_port_id, 0, pkts_burst, MAX_PKT_BURST);
		if (unlikely(nb_rx == 0))
			/*  Nothing received? Continue. */
			continue;

		port_statistics[rx_port_id].rx += nb_rx;

		for (i = 0; i < nb_rx; i++) {
			m = pkts_burst[i];

			/* Drop one in 10 packets, the 5th one. */
			if (i % 10 == 5) {
				/* ToDo: correctly drop based on total RX packets, not
				 * while iterating the burst (e.g. 32 packets burst)
				 */
				rte_pktmbuf_free(m);
				continue;
			}

			rte_prefetch0(rte_pktmbuf_mtod(m, void *));

			buffer = tx_buffer[tx_port_id];

			sent = rte_eth_tx_buffer(tx_port_id, 0, buffer, m);
			if (sent)
				port_statistics[tx_port_id].tx += sent;
		}
	}
}

static int netem_launch_one_lcore(__rte_unused void *dummy)
{
	netem_main_loop();
	return 0;
}

static void signal_handler(int signum)
{
	if (signum == SIGINT || signum == SIGTERM) {
		printf("\n\nSignal %d received, preparing to exit...\n", signum);
		force_quit = true;
	}
}

int main(int argc, char **argv)
{
	int ret;
	uint16_t nb_ports;
	uint16_t nb_ports_available = 0;
	uint16_t portid;
	unsigned lcore_id;
	unsigned int nb_lcores = 2;
	unsigned int nb_mbufs;

	/* Init EAL */
	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Invalid EAL arguments\n");
	argc -= ret;
	argv += ret;

	// Init the packet queue
	queue = init_packet_queue();

	force_quit = false;
	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);

	/* convert to number of cycles */
	timer_period *= rte_get_timer_hz();

	nb_ports = rte_eth_dev_count_avail();
	if (nb_ports == 0)
		rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");

	nb_mbufs = RTE_MAX(nb_ports * (nb_rxd + nb_txd + MAX_PKT_BURST +
								   nb_lcores * MEMPOOL_CACHE_SIZE),
					   8192U);

	/* Create the mbuf pool */
	netem_pktmbuf_pool =
		rte_pktmbuf_pool_create("mbuf_pool", nb_mbufs, MEMPOOL_CACHE_SIZE, 0,
								RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
	if (netem_pktmbuf_pool == NULL)
		rte_exit(EXIT_FAILURE, "Cannot init mbuf pool\n");

	/* Initialize each port */
	RTE_ETH_FOREACH_DEV(portid)
	{
		struct rte_eth_rxconf rxq_conf;
		struct rte_eth_txconf txq_conf;
		struct rte_eth_conf local_port_conf = port_conf;
		struct rte_eth_dev_info dev_info;

		nb_ports_available++;

		/* init port */
		printf("Initializing port %u... ", portid);
		fflush(stdout);

		ret = rte_eth_dev_info_get(portid, &dev_info);
		if (ret != 0)
			rte_exit(EXIT_FAILURE,
					 "Error during getting device (port %u) info: %s\n", portid,
					 strerror(-ret));

		if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
			local_port_conf.txmode.offloads |=
				RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;
		/* Configure the number of queues for a port. */
		ret = rte_eth_dev_configure(portid, 1, 1, &local_port_conf);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "Cannot configure device: err=%d, port=%u\n",
					 ret, portid);

		ret = rte_eth_dev_adjust_nb_rx_tx_desc(portid, &nb_rxd, &nb_txd);
		if (ret < 0)
			rte_exit(EXIT_FAILURE,
					 "Cannot adjust number of descriptors: err=%d, port=%u\n",
					 ret, portid);

		ret = rte_eth_macaddr_get(portid, &netem_ports_eth_addr[portid]);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "Cannot get MAC address: err=%d, port=%u\n",
					 ret, portid);

		/* init one RX queue */
		fflush(stdout);
		rxq_conf = dev_info.default_rxconf;
		rxq_conf.offloads = local_port_conf.rxmode.offloads;
		/* RX queue setup */
		ret = rte_eth_rx_queue_setup(portid, 0, nb_rxd,
									 rte_eth_dev_socket_id(portid), &rxq_conf,
									 netem_pktmbuf_pool);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "rte_eth_rx_queue_setup:err=%d, port=%u\n",
					 ret, portid);

		/* Init one TX queue on each port */
		fflush(stdout);
		txq_conf = dev_info.default_txconf;
		txq_conf.offloads = local_port_conf.txmode.offloads;
		ret = rte_eth_tx_queue_setup(portid, 0, nb_txd,
									 rte_eth_dev_socket_id(portid), &txq_conf);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "rte_eth_tx_queue_setup:err=%d, port=%u\n",
					 ret, portid);

		/* Initialize TX buffers */
		tx_buffer[portid] = rte_zmalloc_socket(
			"tx_buffer", RTE_ETH_TX_BUFFER_SIZE(MAX_PKT_BURST), 0,
			rte_eth_dev_socket_id(portid));
		if (tx_buffer[portid] == NULL)
			rte_exit(EXIT_FAILURE, "Cannot allocate buffer for tx on port %u\n",
					 portid);

		rte_eth_tx_buffer_init(tx_buffer[portid], MAX_PKT_BURST);

		ret = rte_eth_tx_buffer_set_err_callback(
			tx_buffer[portid], rte_eth_tx_buffer_count_callback,
			&port_statistics[portid].dropped);
		if (ret < 0)
			rte_exit(EXIT_FAILURE,
					 "Cannot set error callback for tx buffer on port %u\n",
					 portid);

		ret = rte_eth_dev_set_ptypes(portid, RTE_PTYPE_UNKNOWN, NULL, 0);
		if (ret < 0)
			printf("Port %u, Failed to disable Ptype parsing\n", portid);
		/* Start device */
		ret = rte_eth_dev_start(portid);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "rte_eth_dev_start:err=%d, port=%u\n", ret,
					 portid);

		printf("Port %u, MAC address: " RTE_ETHER_ADDR_PRT_FMT "\n\n", portid,
			   RTE_ETHER_ADDR_BYTES(&netem_ports_eth_addr[portid]));

		/* initialize port stats */
		memset(&port_statistics, 0, sizeof(port_statistics));
	}

	if (!nb_ports_available) {
		rte_exit(EXIT_FAILURE, "No ports available\n");
	}

	ret = 0;
	/* launch per-lcore init on every lcore */
	rte_eal_mp_remote_launch(netem_launch_one_lcore, NULL, CALL_MAIN);
	RTE_LCORE_FOREACH_WORKER(lcore_id)
	{
		if (rte_eal_wait_lcore(lcore_id) < 0) {
			ret = -1;
			break;
		}
	}

	RTE_ETH_FOREACH_DEV(portid)
	{
		printf("Closing port %d...", portid);
		ret = rte_eth_dev_stop(portid);
		if (ret != 0)
			printf("rte_eth_dev_stop: err=%d, port=%d\n", ret, portid);
		rte_eth_dev_close(portid);
		printf(" Done\n");
	}

	/* clean up the EAL */
	rte_eal_cleanup();
	printf("Bye...\n");

	return ret;
}
