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
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of TType for a binary block of data.
	/// </summary>
	[Serializable]
	public class TBinaryType : TType {
		/// <summary>
		/// This constrained size of the binary block of data or -1 if there is no size limit.
		/// </summary>
		private readonly int max_size;

		public TBinaryType(SqlType sql_type, int max_size)
			: base(sql_type) {
			this.max_size = max_size;
		}

		/// <summary>
		/// Returns the maximum size of this binary type.
		/// </summary>
		public int MaximumSize {
			get { return max_size; }
		}

		// ---------- Static utility method for comparing blobs ----------

		/// <summary>
		/// Utility method for comparing one blob with another.
		/// </summary>
		/// <param name="blob1"></param>
		/// <param name="blob2"></param>
		/// <remarks>
		/// Uses the <see cref="IBlobAccessor"/> interface to compare the blobs. 
		/// This will collate larger blobs higher than smaller blobs.
		/// </remarks>
		/// <returns></returns>
		static int CompareBlobs(IBlobAccessor blob1, IBlobAccessor blob2) {
			// We compare smaller sized blobs before larger sized blobs
			int c = blob1.Length - blob2.Length;
			if (c != 0) {
				return c;
			} else {
				// Size of the blobs are the same, so find the first non equal byte in
				// the byte array and return the difference between the two.  eg.
				// compareTo({ 0, 0, 0, 1 }, { 0, 0, 0, 3 }) == -3

				int len = blob1.Length;

				Stream b1 = blob1.GetInputStream();
				Stream b2 = blob2.GetInputStream();
				try {
					BufferedStream bin1 = new BufferedStream(b1);
					BufferedStream bin2 = new BufferedStream(b2);
					while (len > 0) {
						c = bin1.ReadByte() - bin2.ReadByte();
						if (c != 0) {
							return c;
						}
						--len;
					}

					return 0;
				} catch (IOException e) {
					throw new Exception("IO Error when comparing blobs: " +
											   e.Message);
				}
			}
		}

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TBinaryType);
		}

		/// <inheritdoc/>
		public override int Compare(Object ob1, Object ob2) {
			if (ob1 == ob2) {
				return 0;
			}

			IBlobAccessor blob1 = (IBlobAccessor)ob1;
			IBlobAccessor blob2 = (IBlobAccessor)ob2;

			return CompareBlobs(blob1, blob2);
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			if (ob != null) {
				if (ob is IBlobRef)
					return 256;
				return ((ByteLongObject)ob).Length + 24;
			}
			return 32;
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(IBlobAccessor);
		}

	}
}