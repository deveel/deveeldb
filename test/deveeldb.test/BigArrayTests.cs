using System;

using Xunit;

namespace Deveel {
	public static class BigArrayTests {
		[Fact]
		public static void Clear() {
			var array = new BigArray<long>(5);
			array[0] = 22;
			array[1] = 90;
			array[2] = 19920020339484;

			BigArray<long>.Clear(array, 0, 5);

			Assert.Equal(5, array.Length);
			Assert.Equal(0, array[0]);
			Assert.Equal(0, array[1]);
			Assert.Equal(0, array[2]);
		}

		[Fact]
		public static void ResizeGrow() {
			var array = new BigArray<long>(34);
			array.Resize(1002563);

			Assert.Equal(1002563, array.Length);
		}

		[Fact]
		public static void CopyToArray() {
			var array = new BigArray<long>(32);
			array[0] = 33;
			array[2] = 918;

			var dest = new long[5];
			array.CopyTo(0, dest, 0, 5);

			Assert.Equal(33, dest[0]);
			Assert.Equal(918, dest[2]);
		}

		[Fact]
		public static void CopyToBigArray() {
			var array = new BigArray<long>(32);
			array[0] = 33;
			array[2] = 918;

			var dest = new BigArray<long>(34);
			array.CopyTo(2, dest, 0, 30);
			Assert.Equal(34, dest.Length);
			Assert.Equal(918, dest[0]);
		}
	}
}