using System;

namespace Deveel.Data.Util {
	///<summary>
	/// Provides various sort utilities for a list of objects that implement 
	/// <see cref="IComparable"/>.
	///</summary>
	/// <remarks>
	/// It also provide some methods that can be used on a sorted list of objects, 
	/// such as a fast search method.
	/// <para>
	/// All the methods in this class are static.
	/// </para>
	/// </remarks>
	public sealed class SortUtil {
		///<summary>
		///  Performs a quick sort on the given array of Comparable objects between
		/// the min and maximum range.
		///</summary>
		///<param name="list"></param>
		///<param name="min"></param>
		///<param name="max"></param>
		/// <remarks>
		/// It changes the array to the new sorted order.
		/// </remarks>
		public static void QuickSort(IComparable[] list, int min, int max) {
			Array.Sort(list, min, max + 1);

			//    int left = min;
			//    int right = max;
			//
			//    if (max > min) {
			//      Comparable mid = list[(min + max) / 2];
			//      while (left < right) {
			//        while (left < max && list[left].compareTo(mid) < 0) {
			//          ++left;
			//        }
			//        while (right > min && list[right].compareTo(mid) > 0) {
			//          --right;
			//        }
			//        if (left <= right) {
			//          if (left != right) {
			//            Comparable t = list[left];
			//            list[left] = list[right];
			//            list[right] = t;
			//          }
			//
			//          ++left;
			//          --right;
			//        }
			//
			//      }
			//
			//      if (min < right) {
			//        QuickSort(list, min, right);
			//      }
			//      if (left < max) {
			//        QuickSort(list, left, max);
			//      }
			//
			//    }
		}

		///<summary>
		/// Performs a quick sort on the given array of <see cref="IComparable"/> objects.
		///</summary>
		///<param name="obs"></param>
		/// <remarks>
		/// It changes the array to the new sorted order.
		/// </remarks>
		public static void QuickSort(IComparable[] obs) {
			QuickSort(obs, 0, obs.Length - 1);
		}


		///<summary>
		/// Quickly finds the index of the given object in the list.
		///</summary>
		///<param name="list"></param>
		///<param name="val"></param>
		///<param name="lower"></param>
		///<param name="higher"></param>
		/// <remarks>
		/// If the object can not be found, it returns the point where the element 
		/// should be added.
		/// </remarks>
		///<returns></returns>
		public static int SortedIndexOf(IComparable[] list, IComparable val, int lower, int higher) {
			if (lower >= higher)
				return val.CompareTo(list[lower]) > 0 ? lower + 1 : lower;

			int mid = (lower + higher) / 2;
			IComparable mid_val = list[mid];

			if (val.Equals(mid_val))
				return mid;

			return val.CompareTo(mid_val) < 0
			       	? SortedIndexOf(list, val, lower, mid - 1)
			       	: SortedIndexOf(list, val, mid + 1, higher);
		}

		///<summary>
		/// Quickly finds the given element in the array of objects.
		///</summary>
		///<param name="list"></param>
		///<param name="val"></param>
		///<param name="results"></param>
		/// <remarks>
		/// It assumes the object given is of the same type as the array. This is for reuse of 
		/// an old <see cref="SearchResults"/> object that is no longer needed. This prevents 
		/// GC and overhead of having to allocate a new <see cref="SearchResults"/> object.
		/// This method works by dividing the search space in half and iterating in the half 
		/// with the given object in.
		/// </remarks>
		///<returns>
		/// Returns a <see cref="SearchResults"/> object that contains information about the 
		/// index of the found object, and the number of objects like this in the array, or 
		/// <b>null</b> if the given object is not in the array.
		/// </returns>
		public static SearchResults SortedQuickFind(IComparable[] list, IComparable val, SearchResults results) {
			if (list.Length == 0)
				return null;

			int size = list.Length - 1;
			int count = 0;

			int i = SortedIndexOf(list, val, 0, size);
			if (i > size)
				return null;

			int temp_i = i;

			while (temp_i >= 0 && list[temp_i].Equals(val)) {
				++count;
				--temp_i;
			}
			int start_index = temp_i + 1;
			temp_i = i + 1;
			while (temp_i <= size && list[temp_i].Equals(val)) {
				++count;
				++temp_i;
			}

			if (count == 0)
				return null;

			if (results == null)
				results = new SearchResults();

			results.found_index = start_index;
			results.found_count = count;

			return results;
		}
	}
}