/*
 * Copyright (c) 2014 Pavel Kazenin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.pavelkazenin.parallel.util;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/*
 * @author pavel.kazenin@gmail.com
 */
public class ArraysExt {

	/**
	 * Sorts the specified array of longs into ascending numerical order. 
	 * 
	 * @param array      array of longs to be sorted
	 * @param nthreads   specifies the number of threads to use in sort algorithm
	 *                   if nthreads = -1 the method calculates the optimal number of threads  
	 * @return           the number of threads used to sort the array
	 * @throws NullPointerException input array is null or empty
	 */
	public static int parallelSort(long[] array, int nthreads) throws NullPointerException {
		
		if (array == null || array.length == 0) {
			// null or empty array
			throw new NullPointerException("input array is null or empty");
		}
		if (nthreads == 1) {
			Arrays.sort(array);
			return 1;			
		}
		if (nthreads > 1) {
			doParallelSort(array, nthreads);
			return nthreads;
		}
		else { // nthreads <= 0
			int optimalThreads = doCalculateAndParallelSort(array);
			return optimalThreads;
		}
	}
		
	private static int doCalculateAndParallelSort(long[] array) {
		int m = 1;
		int n = 100;
		int v = array.length;
		long[] array2 = Arrays.copyOf(array, v);
		
		long tm = doParallelSort(array2, m);
		long tn = doParallelSort(array, n);
		
		return Calculators.calculateOptimalThreadsQuickSort(v, m, n, tm, tn);
	}
	
	
	private static long doParallelSort(long[] array, int nthreads) {
		
		int volume = array.length;
		double volumeDouble = (double)volume;
		int fromIndex;
		int toIndex;
		
		long[] array2 = Arrays.copyOf(array, volume);
		
	    CountDownLatch startSignal = new CountDownLatch(1);
	    CountDownLatch doneSignal = new CountDownLatch(nthreads);
	
	    ArraysExt sorter = new ArraysExt();
	
	    for (int i = 0; i < nthreads; ++i) {
	  
		    fromIndex = (int)(i*volumeDouble)/nthreads;
		 
		    toIndex = (int)((i+1)*volumeDouble)/nthreads;
		  
		    if (toIndex>volume) toIndex=volume;
		  
		    new Thread(sorter.new WorkerLong(array2, fromIndex, toIndex, startSignal, doneSignal)).start();
	    }
	    
	    long begTime = System.currentTimeMillis();
	    
	    // start all sorting threads
	    startSignal.countDown();
	    
	    // wait for all threads to finish
	    try {
	    	doneSignal.await();
	    } catch (InterruptedException e) {}
	    
	    // merge all sorted subarrays
		int[] end = new int[nthreads];
	    int[] pos = new int[nthreads];
    	  
  	    for (int i = 0; i < nthreads; ++i) {
	    	      
  	    	pos[i] = (int)(i*volumeDouble)/nthreads;
  	    	end[i] = (int)((i+1)*volumeDouble)/nthreads;
	    	      
	    	if (end[i]>volume) end[i]=volume;
	    }
	    	    
	    int mergeIdx;
	    long minValue;
	    int minIdx;
	    	  
	    for (mergeIdx = 0; mergeIdx < volume; mergeIdx++) {

	    	   minValue = Long.MAX_VALUE;
	    	   minIdx = 0;
	    	   for (int i=0; i < nthreads; i++) {
	    	    
	    		   if (pos[i] < end[i]) {
	    			   if (array2[pos[i]] < minValue) {
	    				   minIdx = i;
	    				   minValue = array2[pos[i]];
	    			   }
	    		   }
	    		   pos[minIdx]++;
	    		   array[mergeIdx]=minValue;
	    	   }
	    }
	    
	    long endTime = System.currentTimeMillis();
	    
	    return endTime-begTime;

	}
	
	private class WorkerLong implements Runnable {
		  
		 private long[] array;
		 private final int fromIndex;
		 private final int toIndex;
		 private final CountDownLatch startSignal;
		 private final CountDownLatch doneSignal;	 
		  
		 private WorkerLong( long[] array, int fromIndex, int toIndex,
				  		  CountDownLatch startSignal, CountDownLatch doneSignal ) {

			 this.array = array;
			 this.fromIndex = fromIndex;
			 this.toIndex = toIndex;
			 this.startSignal = startSignal;
			 this.doneSignal = doneSignal;
		 }
		  
		 public void run() {
			   try {
				   startSignal.await();
			 
				   Arrays.sort(array, fromIndex, toIndex);
			    
				   doneSignal.countDown();
			   }
			   catch (InterruptedException e) {}
		  }
	 }
	

	 public static boolean isSorted(long[] array) {
		 
		  for (int i=0; i<array.length-1; i++ ) {
			  if (array[i]>array[i+1]) return false;
		  }
		  
		  return true;
	 }

    /**
	 * Sorts the specified array of objects into ascending order, according to the natural ordering 
	 * of its elements. All elements in the array must implement the Comparable interface. 
	 * Furthermore, all elements in the array must be mutually comparable (that is, e1.compareTo(e2) 
	 * must not throw a ClassCastException for any elements e1 and e2 in the array). 
	 * 
	 * @param array      the array to be sorted
	 * @param nthreads   specifies the number of threads to use in sort algorithm
	 *                   if nthreads = -1 the method calculates the optimal number of threads  
	 * @return           the number of threads used to sort the array
	 * @throws NullPointerException input array is null or empty
	 */
	public static <T> int parallelSort(Comparable<T>[] array, int nthreads) throws NullPointerException {
		
		if (array == null || array.length == 0) {
			// null or empty array
			throw new NullPointerException("input array in null or empty");
		}
		if (nthreads == 1) {
			Arrays.sort(array);
			return 1;			
		}
		if (nthreads > 1) {
			doParallelSort(array, nthreads);
			return nthreads;
		}
		else { // nthreads <= 0
			int optimalThreads = doCalculateAndParallelSort(array);
			return optimalThreads;
		}
	}
		
	private static <T> int doCalculateAndParallelSort(Comparable<T>[] array) {
		int m = 1;
		int n = 100;
		int v = array.length;
		Comparable<T>[] array2 = Arrays.copyOf(array, v);
		
		long tm = doParallelSort(array2, m);
		long tn = doParallelSort(array, n);
		
		return Calculators.calculateOptimalThreadsQuickSort(v, m, n, tm, tn);
	}
	
	
	private static <T> long doParallelSort(Comparable<T>[] array, int nthreads) {
		
		int volume = array.length;
		double volumeDouble = (double)volume;
		int fromIndex;
		int toIndex;
		
		Comparable<T>[] array2 = Arrays.copyOf(array, volume);
		
	    CountDownLatch startSignal = new CountDownLatch(1);
	    CountDownLatch doneSignal = new CountDownLatch(nthreads);
	
	    ArraysExt sorter = new ArraysExt();
	
	    for (int i = 0; i < nthreads; ++i) {
	  
		    fromIndex = (int)(i*volumeDouble)/nthreads;
		 
		    toIndex = (int)((i+1)*volumeDouble)/nthreads;
		  
		    if (toIndex>volume) toIndex=volume;
		  
		    new Thread(sorter.new WorkerComparable(array2, fromIndex, toIndex, startSignal, doneSignal)).start();
	    }
	    
	    long begTime = System.currentTimeMillis();
	    
	    // start all sorting threads
	    startSignal.countDown();
	    
	    // wait for all threads to finish
	    try {
	    	doneSignal.await();
	    } catch (InterruptedException e) {}
	    
	    // merge all sorted subarrays
		int[] end = new int[nthreads];
	    int[] pos = new int[nthreads];
    	  
  	    for (int i = 0; i < nthreads; ++i) {
	    	      
  	    	pos[i] = (int)(i*volumeDouble)/nthreads;
  	    	end[i] = (int)((i+1)*volumeDouble)/nthreads;
	    	      
	    	if (end[i]>volume) end[i]=volume;
	    }
	    	    
	    int mergeIdx;
	    Comparable<T> minValue = null;
	    int minIdx;
	    	  
	    for (mergeIdx = 0; mergeIdx < volume; mergeIdx++) {

	    	   minIdx = 0;
	    	   for (int i=0; i < nthreads; i++) {
	    	    
	    		   if (pos[i] < end[i]) {
	    			   if (mergeIdx == 0 || array2[pos[i]].compareTo((T)minValue) < 0) {
	    				   minIdx = i;
	    				   minValue = array2[pos[i]];
	    			   }
	    		   }
	    		   pos[minIdx]++;
	    		   array[mergeIdx]=minValue;
	    	   }
	    }
	    
	    long endTime = System.currentTimeMillis();
	    
	    return endTime-begTime;

	}
	
	private class WorkerComparable implements Runnable {
		  
		 private Object[] array;
		 private final int fromIndex;
		 private final int toIndex;
		 private final CountDownLatch startSignal;
		 private final CountDownLatch doneSignal;	 
		  
		 private WorkerComparable( Object[] array, int fromIndex, int toIndex,
				  		  CountDownLatch startSignal, CountDownLatch doneSignal ) {

			 this.array = array;
			 this.fromIndex = fromIndex;
			 this.toIndex = toIndex;
			 this.startSignal = startSignal;
			 this.doneSignal = doneSignal;
		 }
		  
		 public void run() {
			   try {
				   startSignal.await();
			 
				   Arrays.sort(array, fromIndex, toIndex);
			    
				   doneSignal.countDown();
			   }
			   catch (InterruptedException e) {}
		  }
	 }	 

	 public static boolean isSorted(Comparable<Object>[] array) {
		 
		  for (int i=0; i<array.length-1; i++ ) {
			  if (array[i].compareTo(array[i+1]) > 0) return false;
		  }
		  
		  return true;
	 }

}
