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

/*
 * @author pavel.kazenin@gmail.com
 */
public class Calculators {

	 /**
	  *  calculate the optimal number of threads for parallel quick sort
	  *  by solving equation x^2 = -a*ln(x) + b numerically
	  *  
	  *  @param	v	volume of data set
	  *  @param	m	first measured number of threads
	  *  @param n	second measured number of threads
	  *  @param	tm	execution time for m number of threads
	  *  @param tn	execution time for n number of threads
	  *  @return nOptimal	optimal number of threads
	  *  @throws	IlligalArgumentException some argument(s) less than/equal to zero
	  */
	 public static int calculateOptimalThreadsQuickSort (
		   int v, int m, int n, long tm, long tn) throws IllegalArgumentException {
		  
		 	if (v <= 0) throw new IllegalArgumentException("illigal arguments, v <= 0");
		 	if (m <= 0) throw new IllegalArgumentException("illigal arguments, m <= 0");
		 	if (n <= 0) throw new IllegalArgumentException("illigal arguments, n <= 0");
		 	if (tm <= 0) throw new IllegalArgumentException("illigal arguments, tm <= 0");
		 	if (tn <= 0) throw new IllegalArgumentException("illigal arguments, tn <= 0");
		 	if (m == n) throw new IllegalArgumentException("illigal arguments, m == n");

			double x = 1;
			
			
			double f1, f2, df1, df2;
			double a = (n*m*(tm*(n-1)-tn*(m-1)))/(tn*n*Math.log(v/m)-tm*m*Math.log(v/n));
			double b = a*(Math.log(v)+1);
			double delta = 1;
			double sigma = 1;
			    
			for (int i=1; Math.abs(sigma) > 0.00001 && i<100; i++) {
			   
			   f1    = x*x;
			   df1   = 2*x;
			   f2    = -1*a*Math.log(x)+b;
			   df2   = -1*a/x;
			   
			   delta = (f1-f2)/(df1-df2);
			   x     = x-delta;
			   sigma = delta/x;
		  }
			
		  return (x == Double.valueOf(Double.NaN) ? -1 : (int)Math.ceil(x));
	 }
	 
	 /**
	  *  calculate the optimal number of threads for parallel bubble sort
	  *  complexity function O(p^k)
	  *  
	  *  @param	v	volume of data set
	  *  @param	m	first measured number of threads
	  *  @param n	second measured number of threads
	  *  @param	tm	execution time for m number of threads
	  *  @param tn	execution time for n number of threads
	  *  @return nOptimal	optimal number of threads
	  *  @throws	IlligalArgumentException some argument(s) less than/equal to zero
	  */
	 public static int calculateOptimalThreadsBubbleSort(
		  int v, int m, int n, long tm, long tn) {

		  return calculateOptimalThreadsPower(v, m, n, tm, tn, 1.5);
	 }

	 /**
	  *  calculate the optimal number of threads for parallel linear algorithm
	  *  complexity function O(p^k)
	  *  
	  *  @param	v	volume of data set
	  *  @param	m	first measured number of threads
	  *  @param n	second measured number of threads
	  *  @param	tm	execution time for m number of threads
	  *  @param tn	execution time for n number of threads
	  *  @return nOptimal	optimal number of threads
	  *  @throws	IlligalArgumentException some argument(s) less than/equal to zero
	  */
	 public static int calculateOptimalThreadsLinear(
		  int v, int m, int n, long tm, long tn) {

		  return calculateOptimalThreadsPower(v, m, n, tm, tn, 1);
	 }

	 /**
	  *  calculate the optimal number of threads for complexity p^k
	  *  
	  *  @param	v	volume of data set
	  *  @param	m	first measured number of threads
	  *  @param n	second measured number of threads
	  *  @param	tm	execution time for m number of threads
	  *  @param tn	execution time for n number of threads
	  *  @param k   complexity power degree
	  *  @return nOptimal	optimal number of threads
	  *  @throws	IlligalArgumentException some argument(s) less than/equal to zero
	  */
	 public static int calculateOptimalThreadsPower(
		  int v, int m, int n, long tm, long tn, double k) {

		  if (v <= 0) throw new IllegalArgumentException("illegal arguments, v <= 0");
		  if (m <= 0) throw new IllegalArgumentException("illegal arguments, m <= 0");
		  if (n <= 0) throw new IllegalArgumentException("illegal arguments, n <= 0");
		  if (tm <= 0) throw new IllegalArgumentException("illegal arguments, tm <= 0");
		  if (tn <= 0) throw new IllegalArgumentException("illegal arguments, tn <= 0");
		  if (m == n) throw new IllegalArgumentException("illegal arguments, m == n");
		  if (k<1) throw new IllegalArgumentException("illegal arguments, k<1");

		  double x = Math.pow(
			     (Math.pow(n,k)*Math.pow(m,k)*k*(tm*(n-1)-(tn*(m-1)))) /
			     (tn*Math.pow(n,k)-tm*Math.pow(m,k)),
			     1/(k+1));
			   
		  return (x == Double.valueOf(Double.NaN) ? -1 : (int)Math.ceil(x));
	 }

}
