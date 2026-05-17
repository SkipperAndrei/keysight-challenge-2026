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
#define DEFAULT_PQ_INDEX NUM_QUEUES

typedef struct packet_t {
	struct rte_mbuf *m;
	uint8_t pq_id;
	uint64_t send_time;
	uint8_t duplicate;
} packet_t;

typedef struct PQ_t {
	unsigned char pattern[PATTERN_SIZE];
	uint8_t pq_id;
	double drop_rate;
	double double_rate;
	uint64_t delay_us;
} PQ_t;

PQ_t pq[NUM_QUEUES + 1];

int load_pq_config()
{
	FILE *f = fopen("config.csv", "r");
	if (!f) {
		fprintf(stderr, "Cannot open config file\n");
		return -1;
	}

	char line[256];
	int count = 0;

	// Skip header line
	if (!fgets(line, sizeof(line), f)) {
		fprintf(stderr, "Empty config file\n");
		fclose(f);
		return -1;
	}

	while (fgets(line, sizeof(line), f) && count <= NUM_QUEUES) {
		// Strip newline
		line[strcspn(line, "\n")] = '\0';

		// Skip blank lines and comments
		if (line[0] == '\0' || line[0] == '#')
			continue;

		PQ_t entry;
		memset(&entry, 0, sizeof(entry));

		// Split pattern field from the rest
		char *comma = strchr(line, ',');
		if (!comma) {
			fprintf(stderr, "Bad line %d: %s\n", count + 2, line);
			fclose(f);
			return -1;
		}
		*comma = '\0';
		char *pattern_str = line;
		char *rest = comma + 1;

		// Parse remaining fields: pq_id, drop_rate, dup_rate, delay_us
		unsigned int pq_id;
		if (sscanf(rest, "%u,%lf,%lf,%lu", &pq_id, &entry.drop_rate,
				   &entry.double_rate, &entry.delay_us) != 4) {

			if (entry.drop_rate < 0 || entry.drop_rate > 1) {
				entry.drop_rate = 0;
			}

			if (entry.double_rate < 0 || entry.double_rate > 1) {
				entry.double_rate = 0;
			}

			fprintf(stderr, "Bad fields on line %d: %s\n", count + 2, rest);
			fclose(f);
			return -1;
		}
		entry.pq_id = (uint8_t)pq_id;

		// Parse pattern
		unsigned int b[PATTERN_SIZE];
		if (sscanf(pattern_str,
				   "%02x:%02x:%02x:%02x:%02x:%02x:"
				   "%02x:%02x:%02x:%02x:%02x:%02x",
				   &b[0], &b[1], &b[2], &b[3], &b[4], &b[5], &b[6], &b[7],
				   &b[8], &b[9], &b[10], &b[11]) != PATTERN_SIZE) {
			fprintf(stderr, "Bad pattern on line %d: %s\n", count + 2,
					pattern_str);
			fclose(f);
			return -1;
		}
		for (int i = 0; i < PATTERN_SIZE; i++)
			entry.pattern[i] = (unsigned char)b[i];

		pq[count++] = entry;
	}

	PQ_t default_entry;
	memset(&default_entry, 0, sizeof(default_entry));
	default_entry.pq_id = DEFAULT_PQ_INDEX;
	pq[DEFAULT_PQ_INDEX] = default_entry; // Last entry is default profile

	fclose(f);

	if (count != NUM_QUEUES) {
		fprintf(stderr, "Expected %d entries, got %d\n", NUM_QUEUES, count);
		return -1;
	}

	printf("Loaded %d PQ entries from config.csv\n", count);
	return 0;
}

typedef struct packet_queue_t {
	struct rte_ring *ring;
	struct rte_mempool *item_pool;
} packet_queue_t;

#define QUEUE_RING_SIZE 4096
packet_queue_t *queues[2];

#define TX_RING_SIZE 4096
static struct rte_ring *tx_ring[NB_PORTS] = { NULL, NULL };

/* ethernet addresses of ports */
static struct rte_ether_addr netem_ports_eth_addr[NB_PORTS];

// Define the maximum number of worker cores your application will use
#define MAX_WORKERS 128

// Change the 1D tx_buffer array into a 2D array: [lcore_id][port_id]
static struct rte_eth_dev_tx_buffer *per_core_tx_buffer[MAX_WORKERS][NB_PORTS];

// Track the actual number of worker cores allocated
static unsigned int nb_workers = 0;

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
	uint64_t duplicated;
};
struct netem_port_statistics port_statistics[NB_PORTS];

/* A tsc-based timer responsible for triggering statistics printout */
static uint64_t timer_period = 1; /* default period is 1 seconds */

packet_queue_t *init_packet_queue(int queue_id)
{
	packet_queue_t *queue =
		rte_zmalloc("PQ_STATE", sizeof(struct packet_queue_t), 0);
	char name_buf[20];

	// Initialize lockless ring optimized for Single-Producer / Single-Consumer mapping
	sprintf(name_buf, "ring_pq_%d", queue_id);
	queue->ring = rte_ring_create(name_buf, QUEUE_RING_SIZE, rte_socket_id(),
								  RING_F_SP_ENQ);

	// Initialize an ultra-fast local object cache pool for your custom struct elements
	sprintf(name_buf, "pool_pq_%d", queue_id);
	queue->item_pool = rte_mempool_create(name_buf, QUEUE_RING_SIZE - 1,
										  sizeof(packet_t), 0, 0, NULL, NULL,
										  NULL, NULL, rte_socket_id(), 0);

	if (!queue->ring || !queue->item_pool) {
		rte_exit(EXIT_FAILURE,
				 "Failed to initialize DPDK pipeline rings or pools\n");
	}

	return queue;
}

int enqueue_packet(struct rte_mbuf *mbuf, PQ_t pq, packet_queue_t *queue,
				   uint8_t duplicate)
{
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
	pkt->send_time = rte_rdtsc();
	pkt->duplicate = duplicate;

	while (rte_ring_enqueue(queue->ring, pkt) < 0);

	return 0;
}

int dequeue_packet(packet_t **pkt, packet_queue_t *queue)
{
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
	for (int p = 0; p < NUM_QUEUES; p++) {
		uint32_t pattern_high = *(uint32_t *)pq[p].pattern;
		uint32_t pattern_mid = *(uint32_t *)(pq[p].pattern + 4);
		uint32_t pattern_low = *(uint32_t *)(pq[p].pattern + 8);

		for (uint32_t i = 0; i < scan_limit; i++) {
			uint32_t pkt_high = *(uint32_t *)(pkt_data + i);
			uint32_t pkt_mid = *(uint32_t *)(pkt_data + i + 4);
			uint32_t pkt_low = *(uint32_t *)(pkt_data + i + 8);

			if (pkt_high == pattern_high && pkt_mid == pattern_mid &&
				pkt_low == pattern_low) {
				return pq[p].pq_id;
			}
		}
	}

	return DEFAULT_PQ_INDEX; // Fallback profile index
}

void worker_process_packet(int queue_id)
{
	uint8_t nb_packets_in_queue = 0;
	uint8_t nb_tx_worker = 0;
	PQ_t pq_info;

	int tx_port_id = queue_id ^ 1;
	unsigned int my_lcore = rte_lcore_id();

	// Calculate a unique TX hardware queue ID assigned to this thread (0 to nb_workers - 1)
	// If your launch arguments include a thread index, use that. Otherwise, map via lcore configuration.
	unsigned int tx_queue_id = rte_lcore_index(my_lcore) - 1;

	struct rte_eth_dev_tx_buffer *buffer =
		per_core_tx_buffer[my_lcore][tx_port_id];
	packet_queue_t *queue = queues[queue_id];

	while (!force_quit) {
		struct packet_t *pkts_burst[MAX_PKT_BURST];

		nb_packets_in_queue = rte_ring_dequeue_burst(
			queue->ring, (void **)pkts_burst, MAX_PKT_BURST, NULL);

		if (unlikely(nb_packets_in_queue == 0))
			continue;

		for (uint8_t i = 0; i < nb_packets_in_queue; i++) {
			struct packet_t *pkt = pkts_burst[i];
			pq_info = pq[pkt->pq_id];

			rte_prefetch0(rte_pktmbuf_mtod(pkt->m, void *));
			if (pq_info.delay_us > 0 &&
				pkt->send_time + US_TO_CYCLES(pq_info.delay_us) > rte_rdtsc()) {
				while (rte_ring_enqueue(queue->ring, pkt) < 0)
                ;
				continue;
			}

			if (pq_info.drop_rate > 0 &&
				rte_rand() / (double)UINT64_MAX < pq_info.drop_rate) {
				rte_pktmbuf_free(pkt->m); // Free the underlying mbuf
				rte_mempool_put(queue->item_pool, pkt); // Free metadata wrapper

				port_statistics[queue_id].dropped++;

				continue;
			}

			if (!pkt->duplicate && pq_info.double_rate > 0 &&
				rte_rand() / (double)UINT64_MAX < pq_info.double_rate) {
				struct rte_mbuf *dup =
					rte_pktmbuf_clone(pkt->m, netem_pktmbuf_pool);
				if (dup != NULL) {
					enqueue_packet(dup, pq_info, queue, 1);
					port_statistics[queue_id].duplicated++;
				}
			}

			// Hand off mbuf to TX thread — no tx_buffer call here
			while (rte_ring_enqueue(tx_ring[tx_port_id], pkt->m) < 0)
				;

			rte_mempool_put(queue->item_pool, pkt);
		}
	}
}

void producer(int rx_port_id)
{
	while (!force_quit) {
		struct rte_mbuf *pkts_burst[MAX_PKT_BURST];
		unsigned nb_rx =
			rte_eth_rx_burst(rx_port_id, 0, pkts_burst, MAX_PKT_BURST);

		if (unlikely(nb_rx == 0))
			/*  Nothing received? Continue. */
			continue;

		port_statistics[rx_port_id].rx += nb_rx;

		for (int i = 0; i < nb_rx; i++) {
			rte_prefetch0(rte_pktmbuf_mtod(pkts_burst[i], void *));
			struct rte_mbuf *m = pkts_burst[i];

			uint8_t pq_idx = classify_packet_to_config_idx(m);
			enqueue_packet(m, pq[pq_idx], queues[rx_port_id], 0);
		}
	}
}

void tx_thread(int tx_port_id)
{
	const uint16_t tx_queue_id = 0;
	const uint64_t drain_tsc =
		(rte_get_tsc_hz() + US_PER_S - 1) / US_PER_S * BURST_TX_DRAIN_US;
	uint64_t prev_tsc = rte_rdtsc();

	unsigned int my_lcore = rte_lcore_id();
	struct rte_eth_dev_tx_buffer *buffer =
		per_core_tx_buffer[my_lcore][tx_port_id];

	if (unlikely(buffer == NULL)) {
		RTE_LOG(ERR, NETEM, "TX thread: no buffer for lcore %u port %u\n",
				my_lcore, tx_port_id);
		return;
	}

	RTE_LOG(INFO, NETEM, "TX thread on lcore %u handling port %u\n", my_lcore,
			tx_port_id);

	while (!force_quit) {
		struct rte_mbuf *mbufs[MAX_PKT_BURST];

		uint16_t nb = rte_ring_dequeue_burst(
			tx_ring[tx_port_id], (void **)mbufs, MAX_PKT_BURST, NULL);

		for (uint16_t i = 0; i < nb; i++) {
			int sent =
				rte_eth_tx_buffer(tx_port_id, tx_queue_id, buffer, mbufs[i]);
			if (sent)
				port_statistics[tx_port_id].tx += sent;
		}

		uint64_t cur_tsc = rte_rdtsc();
		if (unlikely(cur_tsc - prev_tsc > drain_tsc)) {
			int sent = rte_eth_tx_buffer_flush(tx_port_id, tx_queue_id, buffer);
			if (sent)
				port_statistics[tx_port_id].tx += sent;
			prev_tsc = cur_tsc;
		}
	}

	rte_eth_tx_buffer_flush(tx_port_id, tx_queue_id, buffer);
}

/* Print out statistics on packets dropped */
static void print_stats(void)
{
	static uint64_t last_tx[NB_PORTS] = { 0 };
	static uint64_t last_rx[NB_PORTS] = { 0 };
	static uint64_t last_dropped[NB_PORTS] = { 0 };
	static uint64_t last_duplicated[NB_PORTS] = { 0 };
	static uint64_t last_time = 0;

	uint64_t now = rte_get_timer_cycles();

	// On first call, just record the baseline and return
	if (last_time == 0) {
		for (unsigned portid = 0; portid < NB_PORTS; portid++) {
			last_tx[portid] = port_statistics[portid].tx;
			last_rx[portid] = port_statistics[portid].rx;
			last_dropped[portid] = port_statistics[portid].dropped;
			last_duplicated[portid] = port_statistics[portid].duplicated;
		}
		last_time = now;
		return;
	}

	double elapsed = (now - last_time) / (double)rte_get_timer_hz();

	uint64_t total_tx = 0, total_rx = 0;
	uint64_t total_dropped = 0, total_duplicated = 0;

	const char clr[] = { 27, '[', '2', 'J', '\0' };
	const char topLeft[] = { 27, '[', '1', ';', '1', 'H', '\0' };
	printf("%s%s", clr, topLeft);

	printf("\nPort statistics ====================================");

	for (unsigned portid = 0; portid < NB_PORTS; portid++) {
		uint64_t tx = port_statistics[portid].tx;
		uint64_t rx = port_statistics[portid].rx;
		uint64_t drop = port_statistics[portid].dropped;
		uint64_t dup = port_statistics[portid].duplicated;

		double tx_pps = (tx - last_tx[portid]) / elapsed;
		double rx_pps = (rx - last_rx[portid]) / elapsed;
		double drop_pps = (drop - last_dropped[portid]) / elapsed;
		double dup_pps = (dup - last_duplicated[portid]) / elapsed;

		printf("\nStatistics for port %u ------------------------------"
			   "\nPackets sent:        %20" PRIu64 "  (%10.0f pps)"
			   "\nPackets received:    %20" PRIu64 "  (%10.0f pps)"
			   "\nPackets dropped:     %20" PRIu64 "  (%10.0f pps)"
			   "\nPackets duplicated:  %20" PRIu64 "  (%10.0f pps)",
			   portid, tx, tx_pps, rx, rx_pps, drop, drop_pps, dup, dup_pps);

		printf("\nRing occupancy:"
			   "\n  queues[%u]:  %u / %u"
			   "\n  tx_ring[%u]: %u / %u",
			   portid, rte_ring_count(queues[portid]->ring), QUEUE_RING_SIZE,
			   portid, rte_ring_count(tx_ring[portid]), TX_RING_SIZE);

		total_tx += tx;
		total_rx += rx;
		total_dropped += drop;
		total_duplicated += dup;

		last_tx[portid] = tx;
		last_rx[portid] = rx;
		last_dropped[portid] = drop;
		last_duplicated[portid] = dup;
	}

	printf("\nAggregate statistics ==============================="
		   "\nTotal packets sent:        %15" PRIu64
		   "\nTotal packets received:    %15" PRIu64
		   "\nTotal packets dropped:     %15" PRIu64
		   "\nTotal packets duplicated:  %15" PRIu64,
		   total_tx, total_rx, total_dropped, total_duplicated);
	printf("\n====================================================\n");

	last_time = now;
	fflush(stdout);
}

/* main processing loop */
static void netem_main_loop(void)
{
	uint64_t prev_tsc = 0, diff_tsc, cur_tsc, timer_tsc = 0;
	const uint64_t drain_tsc =
		(rte_get_tsc_hz() + US_PER_S - 1) / US_PER_S * BURST_TX_DRAIN_US;

	unsigned lcore_id = rte_lcore_id();
	RTE_LOG(INFO, NETEM, "entering main loop on lcore %u\n", lcore_id);

	while (!force_quit) {
		cur_tsc = rte_rdtsc();
		diff_tsc = cur_tsc - prev_tsc;

		if (unlikely(diff_tsc > drain_tsc)) {
			if (timer_period > 0) {
				timer_tsc += diff_tsc;
				if (unlikely(timer_tsc >= timer_period)) {
					if (lcore_id == rte_get_main_lcore()) {
						print_stats();
						timer_tsc = 0;
					}
				}
			}
			prev_tsc = cur_tsc;
		}
	}
}

static int netem_launch_one_lcore(__rte_unused void *dummy)
{
	unsigned int lcore_id = rte_lcore_id();
	unsigned int lcore_idx = rte_lcore_index(lcore_id);

	if (lcore_idx == 0) {
		netem_main_loop();
	} else if (lcore_idx < 3) {
		producer(lcore_idx - 1);
	} else if (lcore_idx < 5) {
		tx_thread((lcore_idx - 1) % NB_PORTS);
	} else {
		worker_process_packet(lcore_idx % 2);
	}

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

	load_pq_config();

	/* Init EAL */
	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Invalid EAL arguments\n");
	argc -= ret;
	argv += ret;

	// Init the packet queue
	queues[0] = init_packet_queue(0);
	queues[1] = init_packet_queue(1);

	for (int i = 0; i < NB_PORTS; i++) {
		char name[32];
		snprintf(name, sizeof(name), "tx_ring_%d", i);
		tx_ring[i] = rte_ring_create(name, TX_RING_SIZE, rte_socket_id(),
									 RING_F_SC_DEQ); // single consumer per ring
		if (tx_ring[i] == NULL)
			rte_exit(EXIT_FAILURE, "Cannot create tx_ring[%d]\n", i);
	}

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

	nb_workers = 0;
	unsigned int lcore;
	RTE_LCORE_FOREACH_WORKER(lcore)
	{
		nb_workers++;
	}
	// Safety fallback if no worker cores are passed
	if (nb_workers == 0)
		nb_workers = 2;

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

		for (uint16_t q = 0; q < 1; q++) {
			ret = rte_eth_tx_queue_setup(
				portid, q, nb_txd, rte_eth_dev_socket_id(portid), &txq_conf);
			if (ret < 0)
				rte_exit(EXIT_FAILURE,
						 "rte_eth_tx_queue_setup:err=%d, port=%u, queue=%u\n",
						 ret, portid, q);
		}

		/* Initialize TX buffers */
		unsigned int worker_idx = 0;
		RTE_LCORE_FOREACH_WORKER(lcore)
		{
			per_core_tx_buffer[lcore][portid] = rte_zmalloc_socket(
				"tx_buffer", RTE_ETH_TX_BUFFER_SIZE(MAX_PKT_BURST), 0,
				rte_eth_dev_socket_id(portid));

			if (per_core_tx_buffer[lcore][portid] == NULL)
				rte_exit(EXIT_FAILURE,
						 "Cannot allocate buffer for lcore %u on port %u\n",
						 lcore, portid);

			rte_eth_tx_buffer_init(per_core_tx_buffer[lcore][portid],
								   MAX_PKT_BURST);

			// Dynamically link error drops to this port's statistics
			ret = rte_eth_tx_buffer_set_err_callback(
				per_core_tx_buffer[lcore][portid],
				rte_eth_tx_buffer_count_callback,
				&port_statistics[portid].dropped);

			if (ret < 0)
				rte_exit(EXIT_FAILURE, "Cannot set error callback on port %u\n",
						 portid);
		}

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
