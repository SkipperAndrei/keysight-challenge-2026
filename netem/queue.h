/*
 * Copyright (c) 2024, <>
 */

// I do not know who the author is, I added the copyright for the style checker

#ifndef ADD_QUEUE_H
#define ADD_QUEUE_H

#include "utils.h"

typedef struct queue_t {
	unsigned int max_size;
	unsigned int size;
	unsigned int data_size;
	unsigned int read_idx;
	unsigned int write_idx;
	void **buff;
} queue_t;

// @brief Create a new queue
// @param data_size The size of the data
// @param max_size The maximum size of the queue
// @return The new queue
queue_t *q_create(unsigned int data_size, unsigned int max_size);

// @brief Get the size of the queue
// @param q The queue
// @return The size of the queue
unsigned int q_get_size(queue_t *q);

// @brief Check if the queue is empty
// @param q The queue
// @return 1 if the queue is empty, 0 otherwise
unsigned int q_is_empty(queue_t *q);

// @brief Get the front of the queue
// @param q The queue
// @return The front of the queue
void *q_front(queue_t *q);

// @brief Dequeue the front of the queue
// @param q The queue
// @return 1 if the dequeue was successful, 0 otherwise
int q_dequeue(queue_t *q);

// @brief Enqueue a new element in the queue
// @param q The queue
// @param new_data The new data to be enqueued
// @return 1 if the enqueue was successful, 0 otherwise
int q_enqueue(queue_t *q, void *new_data);

// @brief Empty the queue
// @param q The queue
void q_clear(queue_t *q);

// @brief Free the data of the queue
// @param q The queue
void q_free(queue_t *q);

#endif /* ADD_QUEUE_H */
