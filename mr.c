#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

#define STDOUT_FILENO 1

typedef struct mapParameters {
  mapper_t mapperFunct;
  kvlist_t* in;
  kvlist_t* out;
} mapParameters;

typedef struct reduceParameters {
  reducer_t reducerFunct;
  kvlist_t* in;
  kvlist_t* out;
} reduceParameters;

mapParameters* newMapParameters(mapper_t mapper, kvlist_t* inputList,
                                kvlist_t* outputList) {
  mapParameters* mapP = malloc(sizeof(mapParameters));

  if (mapP != NULL) {
    mapP->mapperFunct = mapper;
    mapP->in = inputList;
    mapP->out = outputList;
  }
  return mapP;
}

void freeMapParameters(mapParameters** mP) {
  if (mP != NULL) {
    free(*mP);
  }
}

reduceParameters* newReduceParameters(reducer_t reducer, kvlist_t* inputList,
                                      kvlist_t* outputList) {
  reduceParameters* reduceP = malloc(sizeof(reduceParameters));

  if (reduceP != NULL) {
    reduceP->reducerFunct = reducer;
    reduceP->in = inputList;
    reduceP->out = outputList;
  }
  return reduceP;
}

void freeReduceParameters(reduceParameters** rP) {
  if (rP != NULL) {
    free(*rP);
  }
}

void* mapThreadRoutine(void* p) {
  mapParameters* parameters = (mapParameters*)p;
  kvlist_iterator_t* iterInput = kvlist_iterator_new(parameters->in);

  kvpair_t* currPair = kvlist_iterator_next(iterInput);
  kvlist_t* interOutputList = kvlist_new();
  while (currPair != NULL) {
    parameters->mapperFunct(currPair, parameters->out);

    // parameters->mapperFunct(currPair, interOutputList);

    // // printf("intermediate outputList\n");
    // // kvlist_print(STDOUT_FILENO, interOutputList);
    // // printf("end of list\n");

    // kvlist_extend(parameters->out, interOutputList);

    currPair = kvlist_iterator_next(iterInput);
  }

  kvlist_free(&interOutputList);
  kvlist_iterator_free(&iterInput);

  return NULL;
}

void* reduceThreadRoutine(void* p) {
  // keep reading and adding to a seperate list
  reduceParameters* parameters = (reduceParameters*)p;

  kvlist_iterator_t* iterReduce = kvlist_iterator_new(parameters->in);
  kvpair_t* currPair = kvlist_iterator_next(iterReduce);

  if (currPair == NULL) {  // input list is empty
    kvlist_iterator_free(&iterReduce);
    return NULL;
  }

  char* currWord = currPair->key;
  char* prevWord = NULL;

  kvlist_t* interReduceList = kvlist_new();

  while (currPair != NULL) {
    kvpair_t* copyCurrPair = kvpair_clone(currPair);
    currWord = currPair->key;
    if ((prevWord != NULL) && (strcmp(currWord, prevWord) != 0)) {
      // call reduce function here on interReduceList
      // kvlist_t* testList = kvlist_new();
      parameters->reducerFunct(prevWord, interReduceList, parameters->out);

      // printf("\n\nword: \"%s\"\n", prevWord);

      // printf("interReduceList: \n");

      // kvlist_print(STDOUT_FILENO, interReduceList);

      // printf("---end of interReduceList--- \n");

      // printf("\ntestList: \n");

      // kvlist_print(STDOUT_FILENO, testList);

      // printf("---end of testList--- \n");

      // kvlist_extend(parameters->out, testList);
      // //clear the list

      // kvlist_free(&testList);

      kvlist_free(&interReduceList);
      interReduceList = kvlist_new();

      kvlist_append(interReduceList, copyCurrPair);

    } else {
      kvlist_append(interReduceList, copyCurrPair);
    }

    prevWord = currWord;
    currPair = kvlist_iterator_next(iterReduce);
  }

  // kvlist_t* testList = kvlist_new();
  parameters->reducerFunct(currWord, interReduceList, parameters->out);
  // kvlist_extend(parameters->out, testList);
  // kvlist_free(&testList);
  kvlist_free(&interReduceList);
  kvlist_iterator_free(&iterReduce);

  // printf("\n\nword: \"%s\"\n", prevWord);

  // printf("interReduceList: \n");

  // kvlist_print(STDOUT_FILENO, interReduceList);

  // printf("---end of interReduceList--- \n");

  // printf("\ntestList: \n");

  // kvlist_print(STDOUT_FILENO, testList);

  // printf("---end of testList--- \n");

  // kvlist_free(&testList);

  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  // printf("input list\n");
  // kvlist_print(STDOUT_FILENO, input);

  // return;
  pthread_t mapperThread[num_mapper];
  pthread_t reducerThread[num_reducer];

  kvlist_t** mapperThreadWordLists = calloc(num_mapper, sizeof(kvlist_t*));
  kvlist_t** mappedWordLists =
      calloc(num_mapper, sizeof(kvlist_t*));  // what the threads map to
  kvlist_t** reducerHashTable = calloc(num_reducer, sizeof(kvlist_t*));
  kvlist_t** reducedWordLists = calloc(num_reducer, sizeof(kvlist_t*));

  for (size_t i = 0; i < num_mapper; i++) {
    mapperThreadWordLists[i] = kvlist_new();
    mappedWordLists[i] = kvlist_new();
  }

  for (size_t i = 0; i < num_reducer; i++) {
    reducerHashTable[i] = kvlist_new();
    reducedWordLists[i] = kvlist_new();
  }

  // calculating input size
  size_t inputSize = 0;
  kvlist_iterator_t* iterSize = kvlist_iterator_new(input);
  while (kvlist_iterator_next(iterSize) != NULL) {
    inputSize += 1;
  }
  kvlist_iterator_free(&iterSize);

  // printf("debug: inputSize: %zu\n", inputSize);

  // delegating kvpairs to each list
  kvlist_iterator_t* iterDelegate = kvlist_iterator_new(input);
  size_t listIndex = 0;

  for (size_t i = 1; i <= inputSize; i++) {
    kvpair_t* currPair = kvlist_iterator_next(iterDelegate);

    // printf("debug: currPair: %s %s", currPair->key, currPair->value);
    kvpair_t* copyCurrPair = kvpair_clone(currPair);
    kvlist_append(mapperThreadWordLists[listIndex], copyCurrPair);

    if (listIndex == num_mapper - 1) {
      listIndex = 0;
    } else {
      listIndex += 1;
    }
  }
  kvlist_iterator_free(&iterDelegate);

  // another way to delegate
  //  size_t listIndex = 0;
  //  kvlist_iterator_t* iterDelegate = kvlist_iterator_new(input);
  //  while (1){
  //      kvpair_t* currPair = kvlist_iterator_next(iterDelegate);
  //      if (currPair == NULL) {
  //          break;
  //      }
  //      kvpair_t* copyCurrPair = kvpair_clone(currPair);
  //      kvlist_append(mapperThreadWordLists[listIndex], copyCurrPair);

  //    if (listIndex == num_mapper - 1) {
  //         listIndex = 0;
  //     } else {
  //         listIndex += 1;
  //     }
  // }
  // kvlist_iterator_free(&iterDelegate);

  // printf("\n\nWORD LIST\n\n");

  // for (size_t i = 0; i < num_mapper; i++){
  //     printf("\nthread %lu list\n", i+1);
  //     kvlist_print(STDOUT_FILENO, mapperThreadWordLists[i]);
  //     printf("end of list\n");
  // }

  mapParameters** paramArr = calloc(num_mapper, sizeof(mapParameters*));
  // creating parameter structs
  for (size_t i = 0; i < num_mapper; i++) {
    paramArr[i] =
        newMapParameters(mapper, mapperThreadWordLists[i], mappedWordLists[i]);
    pthread_create(&mapperThread[i], NULL, mapThreadRoutine,
                   (void*)paramArr[i]);
  }

  // wait for threads and free their parameter structs
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_join(mapperThread[i], NULL);
    freeMapParameters(&paramArr[i]);
  }

  free(paramArr);

  // printf("\n\nAFTER MAPPING WORD LISTS\n\n");

  for (size_t i = 0; i < num_mapper; i++) {
    // printf("\nthread %lu list\n", i+1);
    // kvlist_print(STDOUT_FILENO, mappedWordLists[i]);
    // printf("end of list\n");

    kvlist_iterator_t* shuffleIter = kvlist_iterator_new(mappedWordLists[i]);
    kvpair_t* currPair = kvlist_iterator_next(shuffleIter);
    unsigned long hashIndex;
    while (currPair != NULL) {
      hashIndex = (hash(currPair->key)) % num_reducer;

      kvpair_t* copyCurrPair = kvpair_clone(currPair);
      kvlist_append(reducerHashTable[hashIndex], copyCurrPair);
      // kvlist_sort(reducerHashTable[hashIndex]);

      currPair = kvlist_iterator_next(shuffleIter);
    }
    kvlist_iterator_free(&shuffleIter);
  }

  // printf("\n\nINTERMEDIATE HASH TABLE (SHUFFLE PHASE)\n\n");
  // for (size_t i = 0; i < num_reducer; i++){
  //     printf("thread %lu:\n", i+1);

  //     kvlist_print(STDOUT_FILENO, reducerHashTable[i]);
  //     printf("end of list\n\n");

  // }

  // reduce phase, creating the threads and thread struct
  reduceParameters** reduceParamArr =
      calloc(num_reducer, sizeof(reduceParameters*));

  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_sort(reducerHashTable[i]);
    reduceParamArr[i] =
        newReduceParameters(reducer, reducerHashTable[i], reducedWordLists[i]);
    pthread_create(&reducerThread[i], NULL, reduceThreadRoutine,
                   (void*)reduceParamArr[i]);
  }

  // wait for threads and free their parameter structs
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_join(reducerThread[i], NULL);
    freeReduceParameters(&reduceParamArr[i]);
  }

  free(reduceParamArr);

  // printf("\n\n--threads finished---\n\n");

  // printf("\n\nREDUCED OUTPUT LISTS\n\n");
  // for (size_t i = 0; i < num_reducer; i++){
  //     printf("\nthread %lu:\n", i+1);

  //     kvlist_print(STDOUT_FILENO, reducedWordLists[i]);
  //     printf("end of list\n");

  // }

  // adding reduced lists to final output list
  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_extend(output, reducedWordLists[i]);
  }

  // printf("\n\nFINAL OUTPUT LIST\n\n");

  // kvlist_print(STDOUT_FILENO, output);

  // printf("end of list\n\n");

  // freeing 2d arrays

  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_free(&mappedWordLists[i]);
    kvlist_free(&mapperThreadWordLists[i]);
  }

  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_free(&reducerHashTable[i]);
    kvlist_free(&reducedWordLists[i]);
  }

  free(mappedWordLists);
  free(mapperThreadWordLists);
  free(reducerHashTable);
  free(reducedWordLists);

  // int status = pthread_create(&mapperThread[0], NULL, printMessage,
  // (void*)message);

  // pthread_join(mapperThread[0], NULL);

  // printf("inputSize %d\n", inputSize);

  // printf("input\n");
  // kvlist_print(1, input);
  // kvlist_iterator_t* iter = kvlist_iterator_new(input);

  // kvpair_t* breh = kvlist_iterator_next(iter);
  // kvlist_t* mapperOutput = kvlist_new();

  // int counter = num_mapper;

  // kvlist_t** splitLists = calloc(kvlist
  // for (int i = 0; )

  // mapper(breh, mapperOutput);

  // printf("mapper output\n");
  // kvlist_print(1, mapperOutput);

}  ///
