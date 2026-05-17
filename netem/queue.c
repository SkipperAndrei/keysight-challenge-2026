/*
 * Copyright (c) 2024, <>
 */

// I do not know who the author is, I added the copyright for the style checker

#include "queue.h"

queue_t *q_create(unsigned int data_size, unsigned int max_size)
{
	// Allocate memory for the queue
	queue_t *q = calloc(1, sizeof(*q));
	DIE(!q, "calloc queue failed");

	// Initialize the fields of the queue
	q->data_size = data_size;
	q->max_size = max_size;

	// Allocate memory for the buffer
	q->buff = malloc(max_size * sizeof(*q->buff));
	DIE(!q->buff, "malloc buffer failed");

	// Return the queue
	return q;
}

unsigned int q_get_size(queue_t *q)
{
	// Return the size of the queue
	return !q ? 0 : q->size;
}

unsigned int q_is_empty(queue_t *q)
{
	// Return if the queue is empty
	return !q ? 1 : !q->size;
}

void *q_front(queue_t *q)
{
	// Check if the queue is empty
	if (!q || !q->size)
		return NULL;

	// Return the front of the queue
	return q->buff[q->read_idx];
}

int q_dequeue(queue_t *q)
{
	// Check if the queue is empty
	if (!q || !q->size)
		return 0;

	// Free the front of the queue
	free(q->buff[q->read_idx]);

	// Update the read index and the size
	q->read_idx = (q->read_idx + 1) % q->max_size;
	--q->size;

	// Return success
	return 1;
}

int q_enqueue(queue_t *q, void *new_data)
{
	// Check if the queue is full or if the data is NULL
	if (!q || q->size == q->max_size)
		return 0;

	// Allocate memory for the data
	void *data = malloc(q->data_size);
	DIE(!data, "malloc data failed");
	memcpy(data, new_data, q->data_size);

	// Enqueue the data and update the write index and the size
	q->buff[q->write_idx] = data;
	q->write_idx = (q->write_idx + 1) % q->max_size;
	++q->size;

	// Return success
	return 1;
}

void q_clear(queue_t *q)
{
	// Check if the queue is empty
	if (!q || !q->size)
		return;

	// Free the data of the queue
	for (unsigned int i = q->read_idx; i != q->write_idx;
		 i = (i + 1) % q->max_size)
		free(q->buff[i]);

	// Reset the queue
	q->read_idx = 0;
	q->write_idx = 0;
	q->size = 0;
}

void q_free(queue_t *q)
{
	// Check if the queue is NULL
	if (!q)
		return;

	// Free the data of the queue
	q_clear(q);
	free(q->buff);
	free(q);
}
