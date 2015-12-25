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
package com.pavelkazenin.parallel;

import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.pavelkazenin.parallel.util.ArraysExt;

/*
 * @author pavel.kazenin@gmail.com
 */
public class ArraysExtTest {

	private static int longVolume;		 
	private static long[] longArray;
	
	private static int stringVolume;
	private static String[] stringArray;

	private long begTime;
	private long endTime;
	private long elapsedTime;
	private int nOptimal;
	private int nThreads;
	

	@BeforeClass
	public static void arrayExtTestSetup() {
		longVolume = 1000000;
		longArray = new long[longVolume];
		
		stringVolume = 100000;
		stringArray = new String[stringVolume];
	}

	@Test
	public void sortLongArrayTest() {
	
		System.out.println("\nSorting long array, volume = "+longVolume);
		
		randomizeLongArray();
		nThreads = 1;
		begTime = System.currentTimeMillis();		
		nOptimal = ArraysExt.parallelSort(longArray, nThreads);
		endTime = System.currentTimeMillis();		  
		elapsedTime = endTime-begTime;
		logString();
		
		randomizeLongArray();
		nThreads = -1;
		begTime = System.currentTimeMillis();		
		nOptimal = ArraysExt.parallelSort(longArray, nThreads);
		endTime = System.currentTimeMillis();		  
		elapsedTime = endTime-begTime;		
		logString();

		randomizeLongArray();
		nThreads = nOptimal;
		begTime = System.currentTimeMillis();		
		nOptimal = ArraysExt.parallelSort(longArray, nThreads);
		endTime = System.currentTimeMillis();		  
		elapsedTime = endTime-begTime;		
		logString();

		randomizeLongArray();
		nThreads = 100;
		begTime = System.currentTimeMillis();		
		nOptimal = ArraysExt.parallelSort(longArray, nThreads);
		endTime = System.currentTimeMillis();		  
		elapsedTime = endTime-begTime;		
		logString();
	}
	
	@Test
	public void sortStringArrayTest() {
		
		System.out.println("\nSorting String array, volume = "+stringVolume);
		
		randomizeStringArray();
		nThreads = 1;
		begTime = System.currentTimeMillis();		
		nOptimal = ArraysExt.parallelSort(stringArray, nThreads);
		endTime = System.currentTimeMillis();		  
		elapsedTime = endTime-begTime;
		logString();

		randomizeStringArray();
		nThreads = -1;
		begTime = System.currentTimeMillis();		
		nOptimal = ArraysExt.parallelSort(stringArray, nThreads);
		endTime = System.currentTimeMillis();		  
		elapsedTime = endTime-begTime;
		logString();
		
		randomizeStringArray();
		nThreads = nOptimal;
		begTime = System.currentTimeMillis();		
		nOptimal = ArraysExt.parallelSort(stringArray, nThreads);
		endTime = System.currentTimeMillis();		  
		elapsedTime = endTime-begTime;
		logString();

		randomizeStringArray();
		nThreads = 100;
		begTime = System.currentTimeMillis();		
		nOptimal = ArraysExt.parallelSort(stringArray, nThreads);
		endTime = System.currentTimeMillis();		  
		elapsedTime = endTime-begTime;
		logString();

	}

	private static void randomizeLongArray() {
		 Random generator2 = new Random( System.currentTimeMillis() );
	
	     for (int i = 0; i<longArray.length; i++){
	    	 longArray[i]=generator2.nextLong();
	     }
	 }

	private static void randomizeStringArray() {
		 Random generator2 = new Random( System.currentTimeMillis() );
	
	     for (int i = 0; i<stringArray.length; i++){
	    	 stringArray[i]=new String(Long.toString(generator2.nextLong()));
	     }
	 }

	private void logString() { 
			System.out.println(
			"Time to sort Array = " 
		     + elapsedTime + ", input nTreads = " + nThreads + ", returned nThreads = " 
		     + nOptimal + ", sorted = " + ArraysExt.isSorted(longArray));
	}

}
