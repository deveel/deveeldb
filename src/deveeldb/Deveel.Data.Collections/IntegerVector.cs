// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Text;

namespace Deveel.Data.Collections {
	/// <summary>
	/// Similar to the <see cref="System.Collections.ArrayList"/> class, except t
	/// his can only store integer values.
	/// </summary>
	//TODO: suppress this to use a List<int>?
	[Serializable]
	public sealed class IntegerVector {
		/// <summary>
		/// The int array.
		/// </summary>
		private int[] list;

		/// <summary>
		/// The index of the last value of the array.
		/// </summary>
		private int index;

		/// <summary>
		/// The Constructors.
		/// </summary>
		public IntegerVector()
			: this(32) {
		}

		public IntegerVector(int initial_list_size) {
			index = 0;
			list = new int[initial_list_size];
		}

		public IntegerVector(IntegerVector vec) {
			if (vec != null && vec.list != null) {
				list = new int[vec.list.Length];
				index = vec.index;
				Array.Copy(vec.list, 0, list, 0, index);
			} else {
				index = 0;
				list = new int[0];
			}
		}

		public IntegerVector(IIntegerList i_list)
			: this(i_list.Count) {
			if (i_list is AbstractBlockIntegerList) {
				AbstractBlockIntegerList bilist = (AbstractBlockIntegerList)i_list;
				int bill_size = bilist.Count;
				bilist.CopyToArray(list, 0, bill_size);
				index = bill_size;
			} else {
				IIntegerIterator i = i_list.GetIterator();
				// NOTE: We are guarenteed the size of the 'list' array matches the size
				//   of input list.
				while (i.MoveNext()) {
					list[index] = i.Next;
					++index;
				}
			}
		}


		/// <summary>
		/// Ensures there's enough room to make a single addition to the list.
		/// </summary>
		private void EnsureCapacityForAddition() {
			if (index >= list.Length) {
				int[] old_arr = list;

				int grow_size = old_arr.Length + 1;
				// Put a cap on the new size.
				if (grow_size > 35000) {
					grow_size = 35000;
				}

				int new_size = old_arr.Length + grow_size;
				list = new int[new_size];
				Array.Copy(old_arr, 0, list, 0, index);
			}
		}

		/// <summary>
		/// Ensures there's enough room to make 'n' additions to the list.
		/// </summary>
		/// <param name="n"></param>
		private void EnsureCapacityForAdditions(int n) {
			int intended_size = index + n;
			if (intended_size > list.Length) {
				int[] old_arr = list;

				int grow_size = old_arr.Length + 1;
				// Put a cap on the new size.
				if (grow_size > 35000) {
					grow_size = 35000;
				}

				int new_size = System.Math.Max(old_arr.Length + grow_size, intended_size);
				list = new int[new_size];
				Array.Copy(old_arr, 0, list, 0, index);
			}
		}

		///<summary>
		/// Adds an int to the vector.
		///</summary>
		///<param name="val"></param>
		public void AddInt(int val) {
			//    if (list == null) {
			//      list = new int[64];
			//    }

			EnsureCapacityForAddition();

			list[index] = val;
			++index;
		}

		///<summary>
		/// Removes an Int from the specified position in the list.
		///</summary>
		///<param name="pos"></param>
		public void RemoveIntAt(int pos) {
			--index;
			Array.Copy(list, pos + 1, list, pos, (index - pos));
		}

		///<summary>
		/// Removes the first Int found that matched the specified value.
		///</summary>
		///<param name="val"></param>
		///<exception cref="Exception"></exception>
		public void RemoveInt(int val) {
			int pos = IndexOf(val);
			if (pos == -1) {
				throw new Exception("Tried to remove none existant int.");
			}
			RemoveIntAt(pos);
		}

		/**
		 *   So;
		 *   crop({ 4, 5, 4, 3, 9, 7 }, 0, 3)
		 *   would return {4, 5, 4)
		 * and,
		 *   crop({ 4, 5, 4, 3, 9, 7 }, 3, 4)
		 *   would return {3}
		 */
		///<summary>
		/// Crops the IntegerVector so it only contains values between start 
		/// (inclusive) and end (exclusive).
		///</summary>
		///<param name="start"></param>
		///<param name="end"></param>
		///<exception cref="ApplicationException"></exception>
		public void Crop(int start, int end) {
			if (start < 0) {
				throw new ApplicationException("Crop start < 0.");
			} else if (start == 0) {
				if (end > index) {
					throw new ApplicationException("Crop end was past end.");
				}
				index = end;
			} else {
				if (start >= index) {
					throw new ApplicationException("start >= index");
				}
				int length = (end - start);
				if (length < 0) {
					throw new ApplicationException("end - start < 0");
				}
				Array.Copy(list, start, list, 0, length);
				index = length;
			}
		}

		///<summary>
		/// Inserts an int at the given position.
		///</summary>
		///<param name="val"></param>
		///<param name="pos"></param>
		///<exception cref="ArgumentOutOfRangeException"></exception>
		public void InsertIntAt(int val, int pos) {
			if (pos >= index) {
				throw new ArgumentOutOfRangeException(pos + " >= " + index);
			}

			//    if (list == null) {
			//      list = new int[64];
			//    }

			EnsureCapacityForAddition();
			Array.Copy(list, pos, list, pos + 1, (index - pos));
			++index;
			list[pos] = val;
		}

		///<summary>
		/// Sets an int at the given position, overwriting anything that was
		/// previously there.
		///</summary>
		///<param name="val"></param>
		///<param name="pos"></param>
		///<returns>
		/// Returns the value that was previously at the element.
		/// </returns>
		///<exception cref="ArgumentOutOfRangeException"></exception>
		public int SetIntAt(int val, int pos) {
			if (pos >= index) {
				throw new ArgumentOutOfRangeException(pos + " >= " + index);
			}

			int old = list[pos];
			list[pos] = val;
			return old;
		}

		///<summary>
		/// Places an int at the given position, overwriting anything that was
		/// previously there.
		///</summary>
		///<param name="val"></param>
		///<param name="pos"></param>
		/// <remarks>
		/// If <see cref="pos"/> points to a place outside the bounds of the list 
		/// then the list is expanded to include this value.
		/// </remarks>
		///<returns>
		/// Returns the value that was previously at the element.
		/// </returns>
		public int PlaceIntAt(int val, int pos) {
			int llength = list.Length;
			if (pos >= list.Length) {
				EnsureCapacityForAdditions((llength - index) + (pos - llength) + 5);
			}

			if (pos >= index) {
				index = pos + 1;
			}

			int old = list[pos];
			list[pos] = val;
			return old;
		}


		///<summary>
		/// Appends an IntegerVector to the end of the array.
		///</summary>
		///<param name="vec"></param>
		///<returns>
		/// Returns this object.
		/// </returns>
		public IntegerVector Append(IntegerVector vec) {
			if (vec != null) {
				int size = vec.Count;
				// Make sure there's enough room for the new array
				EnsureCapacityForAdditions(size);

				// Copy the list into this vector.
				Array.Copy(vec.list, 0, list, index, size);
				index += size;

				//      int size = vec.size();
				//      for (int i = 0; i < size; ++i) {
				//        Add(vec.IntAt(i));
				//      }
			}
			return this;
		}

		///<summary>
		/// Returns the Int at the given position.
		///</summary>
		///<param name="pos"></param>
		///<returns></returns>
		///<exception cref="ArgumentOutOfRangeException"></exception>
		public int this[int pos] {
			get {
				if (pos >= index)
					throw new ArgumentOutOfRangeException(pos + " >= " + index);
				return list[pos];
			}
		}

		///<summary>
		///</summary>
		///<param name="val"></param>
		///<returns>
		/// Returns the first index of the given row in the array, or -1 if not found.
		/// </returns>
		public int IndexOf(int val) {
			for (int i = 0; i < index; ++i) {
				if (list[i] == val) {
					return i;
				}
			}
			return -1;
		}

		///<summary>
		///</summary>
		///<param name="val"></param>
		///<returns>
		/// Returns true if the vector contains the given value.
		/// </returns>
		public bool Contains(int val) {
			return (IndexOf(val) != -1);
		}

		/// <summary>
		/// Returns the size of the vector.
		/// </summary>
		public int Count {
			get { return index; }
		}

		///<summary>
		/// Converts the vector into an int[] array.
		///</summary>
		///<returns></returns>
		public int[] ToIntArray() {
			if (Count != 0) {
				int[] out_list = new int[Count];
				Array.Copy(list, 0, out_list, 0, Count);
				return out_list;
			}
			return null;
		}

		///<summary>
		/// Clears the object to be re-used.
		///</summary>
		public void Clear() {
			index = 0;
		}

		/// <summary>
		/// Converts the vector into a String.
		/// </summary>
		/// <returns></returns>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			for (int i = 0; i < index; ++i) {
				buf.Append(list[i]);
				buf.Append(", ");
			}
			return buf.ToString();
		}

		///<summary>
		///</summary>
		///<param name="ivec"></param>
		///<returns>
		/// Returns true if this vector is equal to the given vector.
		/// </returns>
		public bool Equals(IntegerVector ivec) {
			int dest_index = ivec.index;
			if (index != dest_index) {
				return false;
			}
			for (int i = 0; i < index; ++i) {
				if (list[i] != ivec.list[i]) {
					return false;
				}
			}
			return true;
		}

		///<summary>
		/// Reverses all the list of integers.
		///</summary>
		/// <remarks>
		/// So integer[0] is swapped with integer[n - 1], integer[1] is swapped 
		/// with integer[n - 2], etc where n is the size of the vector.
		/// </remarks>
		public void Reverse() {
			int upper = index - 1;
			int bounds = index / 2;
			int end_index, temp;

			// Swap ends and interate the two end pointers inwards.
			// i         = lower end
			// upper - i = upper end

			for (int i = 0; i < bounds; ++i) {
				end_index = upper - i;

				temp = list[i];
				list[i] = list[end_index];
				list[end_index] = temp;
			}
		}




		// These methods are algorithms that can be used on the array, such as
		// sorting and searching.

		///<summary>
		/// Performs a quick sort on the array between the min and max bounds.
		///</summary>
		///<param name="min"></param>
		///<param name="max"></param>
		public void QuickSort(int min, int max) {
			int left = min;
			int right = max;

			if (max > min) {
				int mid = list[(min + max) / 2];
				while (left < right) {
					while (left < max && list[left] < mid) {
						++left;
					}
					while (right > min && list[right] > mid) {
						--right;
					}
					if (left <= right) {
						if (left != right) {
							int t = list[left];
							list[left] = list[right];
							list[right] = t;
						}

						++left;
						--right;
					}

				}

				if (min < right) {
					QuickSort(min, right);
				}
				if (left < max) {
					QuickSort(left, max);
				}

			}
		}

		/// <summary>
		/// Performs a quick sort on the entire vector.
		/// </summary>
		public void QuickSort() {
			QuickSort(0, index - 1);
		}

		///<summary>
		/// This is a very quick search for a value given a sorted array.
		///</summary>
		///<param name="val"></param>
		///<param name="lower"></param>
		///<param name="higher"></param>
		/// <remarks>
		/// The search is performed between the lower and higher bounds of the array. 
		/// If the requested value is not found, it returns the index where the value 
		/// should be 'inserted' to maintain a sorted list.
		/// </remarks>
		///<returns></returns>
		public int SortedIndexOf(int val, int lower, int higher) {
			if (lower >= higher) {
				if (lower < index && val > list[lower]) {
					return lower + 1;
				} else {
					return lower;
				}
			}

			int mid = (lower + higher) / 2;
			int mid_val = list[mid];

			if (val == mid_val) {
				return mid;
			} else if (val < mid_val) {
				return SortedIndexOf(val, lower, mid - 1);
			} else {
				return SortedIndexOf(val, mid + 1, higher);
			}

		}

		///<summary>
		/// Searches the entire sorted list for the given value and returns the 
		/// index of it.
		///</summary>
		///<param name="val"></param>
		/// <remarks>
		/// If the value is not found, it returns the place in the list where the 
		/// value should be insorted to maintain a sorted list.
		/// </remarks>
		///<returns></returns>
		public int SortedIndexOf(int val) {
			return SortedIndexOf(val, 0, index - 1);
		}

		///<summary>
		/// Given a sorted list, this will return the count of this value in the list.
		///</summary>
		///<param name="val"></param>
		/// <remarks>
		/// This uses a quick search algorithm so should be quite fast.
		/// </remarks>
		///<returns></returns>
		public int SortedIntCount(int val) {
			if (index == 0) {
				return 0;
			}

			int count = 0;
			int size = index - 1;

			int i = SortedIndexOf(val, 0, size);
			if (i > size) {
				return 0;
			}
			int temp_i = i;

			while (temp_i >= 0 && list[temp_i] == val) {
				++count;
				--temp_i;
			}
			temp_i = i + 1;
			while (temp_i <= size && list[temp_i] == val) {
				++count;
				++temp_i;
			}

			return count;

		}



		///<summary>
		/// Test routine to check vector is sorted.
		///</summary>
		public bool IsSorted {
			get {
				int cur = Int32.MinValue; //-1000000;
				for (int i = 0; i < index; ++i) {
					int a = list[i];
					if (a >= cur) {
						cur = a;
					} else {
						return false;
					}
				}
				return true;
			}
		}
	}
}