# Project 2: MapReduce

## Goal
The goal of this project is to build a system that involves multiple threads, providing hands-on experience with parallel computing and synchronization mechanisms.

## Introduction to MapReduce

### Motivation
Modern computers come equipped with multiple cores, allowing for parallel processing. Utilizing this parallelism requires the use of threads and synchronization mechanisms, which can be challenging and prone to issues like deadlocks. MapReduce offers a programming model that simplifies parallel processing by abstracting the complexity of distributing work across multiple computational entities.

### Programming Model
In the MapReduce model, computation takes a set of input key-value pairs and produces a set of output key-value pairs. The developer implements this computation using two functions: `Map` and `Reduce`.

- **Map Function**: Takes an input pair and produces a set of intermediate key-value pairs.
- **Reduce Function**: Accepts an intermediate key and a set of all key-value pairs with that key, merges the values, and outputs a set of resulting key-value pairs.

### Example: Word-Counting
Consider the problem of counting the occurrences of each word in a list of text files. 

- **Input**: A list of text files.
- **Map Function**: Splits each value by delimiters and produces intermediate key-value pairs for each word with a count of 1.
- **Reduce Function**: Merges all intermediate key-value pairs with the same key and sums the counts.

For example, given the inputs:
```
[("file1", "hello world"), ("file2", "good afternoon world")]
```

The Map function produces:
```
[("hello", 1), ("world", 1), ("good", 1), ("afternoon", 1), ("world", 1)]

The Reduce function groups and sums these to produce:

```
[("hello", 1), ("world", 2), ("good", 1), ("afternoon", 1)]
```
## Project Details
The project involves implementing a MapReduce-style multi-threaded data processing library. The primary function to implement is `map_reduce` with the following signature:
```c
void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer, size_t num_reducer, kvlist_t* input, kvlist_t* output);
```

## Data Structures
Key data structures provided include:

- **kvpair_t**: Represents a pair of strings (key and value).
- **kvlist_t**: Represents a list of key-value pairs, implemented as a singly-linked list.
- **kvlist_iterator_t**: An iterator for `kvlist_t`.

## Helper Functions
Key helper functions include:

- **kvpair_new**, **kvpair_free**, **kvpair_update_value**
- **kvlist_new**, **kvlist_free**, **kvlist_append**, **kvlist_extend**, **kvlist_sort**, **kvlist_print**
- **kvlist_iterator_new**, **kvlist_iterator_next**, **kvlist_iterator_free**
- **hash**: A simple hash function to distribute results of `map` to reducers.

## Mapper and Reducer
- **Mapper Function**: Takes a key-value pair and writes output to a list.
- **Reducer Function**: Takes a key, a list of key-value pairs, and another list for the output.

## Implementation Phases
1. **Split Phase**: Split the input list into smaller lists for each mapper thread.
2. **Map Phase**: Execute the map function using multiple threads.
3. **Shuffle Phase**: Shuffle mapper results to lists for reducers.
4. **Reduce Phase**: Execute the reduce function using multiple threads.
5. **Output Phase**: Concatenate the results into a single list.

## Additional Requirements
- Use POSIX threads (`pthread.h`).
- No memory leaks (validated using `valgrind`).
- Proper formatting using `clang-format`.


## Acknowledgments
This project and its documentation were developed by Shun Kashiwa.
