using System;
using System.Collections.Generic;
using System.Text;

using Xunit;

namespace Deveel {
	public static class BigListTests {
		[Fact]
		public static void AddRangeOf2000FromBigArray() {
			var array = new BigArray<long>(2000);
			for (int i = 0; i < 2000; i++) {
				array[i] = i * 2;
			}

			var list = new BigList<long>();
			list.AddRange(array);

			Assert.Equal(2000, list.Count);
			Assert.Equal(1999 * 2, list[list.Count - 1]);
		}

		[Fact]
		public static void AddRangeOf2000FromArray() {
			var array = new long[2000];
			for (int i = 0; i < 2000; i++) {
				array[i] = i * 2;
			}

			var list = new BigList<long>();
			list.AddRange(array);

			Assert.Equal(2000, list.Count);
			Assert.Equal(1999 * 2, list[list.Count - 1]);
		}

		[Fact]
		public static void AddRangeOf2000FromList() {
			var array = new List<long>(2000);
			for (int i = 0; i < 2000; i++) {
				array.Add(i * 2);
			}

			var list = new BigList<long>();
			list.AddRange(array);

			Assert.Equal(2000, list.Count);
			Assert.Equal(1999 * 2, list[list.Count - 1]);
		}

		[Fact]
		public static void Construct2000FromBigArray() {
			var array = new BigArray<long>(2000);
			for (int i = 0; i < 2000; i++) {
				array[i] = i * 2;
			}

			var list = new BigList<long>(array);

			Assert.Equal(2000, list.Count);
			Assert.Equal(1999 * 2, list[list.Count - 1]);
		}

		[Fact]
		public static void Construct2000FromArray() {
			var array = new long[2000];
			for (int i = 0; i < 2000; i++) {
				array[i] = i * 2;
			}

			var list = new BigList<long>(array);

			Assert.Equal(2000, list.Count);
			Assert.Equal(1999 * 2, list[list.Count - 1]);
		}

		[Fact]
		public static void InsertAt() {
			var list = new BigList<long>(2);
			list.Add(3);
			list.Add(5);
			list.Insert(0, 6);

			Assert.Equal(6, list[0]);
			Assert.Equal(3, list[1]);
			Assert.Equal(5, list[2]);
		}

		[Fact]
		public static void RemoveAt() {
			var list = new BigList<long>(2);
			list.Add(3);
			list.Add(5);

			list.RemoveAt(1);
			Assert.Equal(1, list.Count);
		}

		[Fact]
		public static void Clear() {
			var list = new BigList<long>(2);
			list.Add(3);
			list.Add(5);

			list.Clear();

			Assert.Empty(list);
		}

		[Fact]
		public static void Contains() {
			var list = new BigList<long>(2);
			list.Add(3);
			list.Add(5);

			Assert.True(list.Contains(5));
			Assert.False(list.Contains(43));
		}

		[Fact]
		public static void Sort() {
			var list = new BigList<long>(2);
			list.Add(12);
			list.Add(5);
			list.Add(30);

			list.Sort();

			Assert.Equal(5, list[0]);
			Assert.Equal(12, list[1]);
			Assert.Equal(30, list[2]);
		}

		[Fact]
		public static void IndexOf() {
			var list = new BigList<long>(2);
			list.Add(13);
			list.Add(50);

			Assert.Equal(1, list.IndexOf(50));
			Assert.Equal(-1, list.IndexOf(102));
		}

		[Fact]
		public static void CopyTo() {
			var list = new BigList<long>(2);
			list.Add(13);
			list.Add(50);
			list.Add(1);

			var array = new long[3];
			list.CopyTo(0, array, array.Length);

			Assert.Equal(13, array[0]);
		}

		[Fact]
		public static void Remove() {
			var list = new BigList<long>(2);
			list.Add(13);
			list.Add(50);
			list.Add(1);

			list.Remove(50);

			Assert.Equal(2, list.Count);
		}

		[Fact]
		public static void TrimToSize() {
			var list = new BigList<long>(2);
			list.Add(13);
			list.Add(50);
			list.Add(1);

			list.TrimToSize(2);
			Assert.Equal(2, list.Count);
		}
	}
}