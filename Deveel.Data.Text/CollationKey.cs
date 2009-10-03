// 
//  CollationKey.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Text;

namespace Deveel.Data.Text {
	/// <summary>
	/// A <see cref="CollationKey"/> represents a <see cref="string"/> under 
	/// the rules of a specific <see cref="ICollator"/> object. 
	/// </summary>
	/// <remarks>
	/// Comparing two <code>CollationKey</code>s returns the relative order of 
	/// the <see cref="string"/>s they represent.
	/// <para>
	/// Since the rule set of <see cref="ICollator"/>s can differ, the sort 
	/// orders of the same string under two different <see cref="ICollator"/>s 
	/// might differ. Hence comparing <see cref="CollationKey"/>s generated 
	/// from different <see cref="ICollator"/>s can give incorrect results.
	/// </para>
	/// <para>
	/// Both the method <see cref="CollationKey.CompareTo(CollationKey)"/> and 
	/// the method <see cref="ICollator.Compare(String, String)"/> compare two 
	/// strings and returns their relative order. The performance characterictics
	/// of these two approaches can differ.
	/// </para>
	/// <para>
	/// During the construction of a <see cref="CollationKey"/>, the entire 
	/// source string is examined and processed into a series of bits terminated 
	/// by a null, that are stored in the <see cref="CollationKey"/>. 
	/// When <see cref="CollationKey.CompareTo(CollationKey)"/> executes, it
	/// performs bitwise comparison on the bit sequences. This can incurs 
	/// startup cost when creating the <see cref="CollationKey"/>, but once the 
	/// key is created, binary comparisons are fast. This approach is recommended 
	/// when the same strings are to be compared over and over again.
	/// </para>
	/// <para>
	/// On the other hand, implementations of <see cref="ICollator.Compare(String, String)"/>
	/// can examine and process the strings only until the first characters 
	/// differing in order. This approach is recommended if the strings are to 
	/// be compared only once.
	/// </para>
	/// <para>
	/// More information about the composition of the bit sequence can be found 
	/// in the <a href="http://icu.sourceforge.net/userguide/Collate_ServiceArchitecture.html">
	/// user guide</a>.
	/// </para>
	/// <para>
	/// The following example shows how <see cref="CollationKey"/>s can be used
	/// to sort a list of <see cref="String"/>s.
	/// </para>
	/// </remarks>
	/// <example>
	/// <code>
	/// // Create an array of CollationKeys for the Strings to be sorted.
	/// ICollator myCollator = new Collator();
	/// CollationKey[] keys = new CollationKey[3];
	/// keys[0] = myCollator.GetCollationKey("Tom");
	/// keys[1] = myCollator.GetCollationKey("Dick");
	/// keys[2] = myCollator.GetCollationKey("Harry");
	/// Sort( keys );
	///  
	/// //...
	/// 
	/// // Inside body of sort routine, compare keys this way
	/// if( keys[i].CompareTo( keys[j] ) > 0 )
	///    // swap keys[i] and keys[j]
	///    
	/// //...
	/// 
	/// // Finally, when we've returned from sort.
	/// Console.Out.WriteLine(keys[0].SourceString);
	/// Console.Out.WriteLine(keys[1].SourceString);
	/// Console.Out.WriteLine(keys[2].SourceString);
	/// </code>
	/// </example>
	public sealed class CollationKey : IComparable {
		#region .ctor
		public CollationKey(string source, byte[] key) {
			m_source_ = source;
			m_key_ = key;
			m_hashCode_ = 0;
			m_length_ = -1;
		}
		#endregion

		#region Fields
		// Sequence of bytes that represents the sort key
		private byte[] m_key_;    
		// Source string this CollationKey represents
		private String m_source_;
		// Hash code for the key
		private int m_hashCode_;
		// Gets the length of this CollationKey
		private int m_length_;
		// Collation key merge seperator
		private const int MERGE_SEPERATOR_ = 2;
		private const byte SORT_LEVEL_TERMINATOR_ = 1;
		#endregion

		#region Properties
		/// <summary>
		/// Gets the source string that this CollationKey represents.
		/// </summary>
		/// <value>
		/// Source string that this CollationKey represents.
		/// </value>
		public string SourceString {
			get { return m_source_; }
		}
		#endregion

		#region Private Methods
		private int GetLength() {
			if (m_length_ >= 0) {
				return m_length_;
			}
			int length = m_key_.Length;
			for (int index = 0; index < length; index ++) {
				if (m_key_[index] == 0) {
					length = index;
					break;
				}
			}
			m_length_ = length;
			return m_length_;
		}
		#endregion

		#region Public Methods
		/// <summary>
		/// Duplicates and returns the value of this CollationKey as a sequence 
		/// of big-endian bytes terminated by a null.
		/// </summary>
		/// <returns></returns>
		/// <example>
		/// If two CollationKeys can be legitimately compared, then one can
		/// compare the byte arrays of each to obtain the same result, e.g.
		/// <code>
		/// byte[] key1 = collationkey1.ToByteArray();
		/// byte[] key2 = collationkey2.ToByteArray();
		/// int key, targetkey;
		/// int i = 0;
		/// do {
		///		key = key1[i] & 0xFF;
		///		targetkey = key2[i] & 0xFF;
		///		if (key &lt; targetkey) {
		///			Console.Out.WriteLine("String 1 is less than string 2");
		///			return;
		///		}
		///		if (targetkey &lt; key) {
		///			Console.Out.WriteLine("String 1 is more than string 2");
		///		}
		///		i ++;
		///	} while (key != 0 && targetKey != 0);
		/// 
		/// Console.Out.WriteLine("Strings are equal.");
		/// </code>
		/// </example>
		public byte[] ToByteArray() {
			int length = 0;
			while (true) {
				if (m_key_[length] == 0) {
					break;
				}
				length ++;
			}
			length ++;
			byte[] result = new byte[length];
			Array.Copy(m_key_, 0, result, 0, length);
			return result;
		}

		public int CompareTo(object obj) {
			return CompareTo((CollationKey)obj);
		}

		/// <summary>
		/// Compare this CollationKey to the given one.
		/// </summary>
		/// <param name="key"></param>
		/// <remarks>
		/// The collation rules of the Collator that created this key 
		/// are applied.
		/// <para>
		/// <b>Note:</b> Comparison between <see cref="CollationKey"/> created by 
		/// different <see cref="ICollator">collators</see> might return incorrect 
		/// results.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public int CompareTo(CollationKey key) {
			for (int i = 0;; ++i) {
				int l = m_key_[i] & 0xff;
				int r = key.m_key_[i] & 0xff;
				if (l < r)
					return -1;
				if (l > r)
					return 1;
				if (l == 0)
					return 0;
			}
		}

		public override bool Equals(object obj) {
			if (!(obj is CollationKey))
				return false;
        
			return Equals((CollationKey)obj);
		}

		public bool Equals(CollationKey key) {
			if (this == key)
				return true;
			if (key == null)
				return false;

			int i = 0;
			while (true) {
				if (m_key_[i] != key.m_key_[i]) {
					return false;
				}
				if (m_key_[i] == 0) {
					break;
				}
				i ++;
			}
			return true;
		}

		public override int GetHashCode() {
			if (m_hashCode_ == 0) {
				if (m_key_ == null) {
					m_hashCode_ = 1;
				}
				else {
					int size = m_key_.Length >> 1;
					StringBuilder key = new StringBuilder(size);
					int i = 0;
					while (m_key_[i] != 0 && m_key_[i + 1] != 0) {
						key.Append((char)((m_key_[i] << 8) | m_key_[i + 1]));
						i += 2;
					}
					if (m_key_[i] != 0) {
						key.Append((char)(m_key_[i] << 8));
					}
					m_hashCode_ = key.ToString().GetHashCode();
				}
			}
			return m_hashCode_;
		}

		/// <summary>
		/// Produce a bound for the sort order of a given collation key and a 
		/// strength level.
		/// </summary>
		/// <param name="boundType"></param>
		/// <param name="noOfLevels"></param>
		/// <remarks>
		/// This method does not attempt to find a bound for the <see cref="CollationKey"/> 
		/// string representation, hence null will be returned in its place.
		/// <para>
		/// Resulting bounds can be used to produce a range of strings that are
		/// between upper and lower bounds. For example, if bounds are produced
		/// for a sortkey of string "smith", strings between upper and lower 
		/// bounds with primary strength would include "Smith", "SMITH", "sMiTh".
		/// </para>
		/// <para>
		/// There are two upper bounds that can be produced. If <see cref="CollationBoundMode.Upper"/>
		/// is produced, strings matched would be as above. However, if a bound is 
		/// produced using <see cref="CollationBoundMode.UpperLong"/> is used, the above 
		/// example will also match "Smithsonian" and similar.
		/// </para>
		/// <para>
		/// For more on usage, see example in test procedure 
		/// <a href=http://oss.software.ibm.com/cvs/icu4j/~checkout~/icu4j/src/com/ibm/icu/dev/test/collator/CollationAPITest.java?rev=&content-type=text/plain>
		/// src/com/ibm/icu/dev/test/collator/CollationAPITest/TestBounds.</a>
		/// </para>
		/// <para>
		/// Collation keys produced may be compared using the <see cref="ICollation.Compare"/>.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the result bounded <see cref="CollationKey"/> with a valid sort order 
		/// but a <b>null</b> string representation.
		/// </returns>
		/// <exception cref="ArgumentException">
		/// Thrown when the strength level requested is higher than or equal to the strength 
		/// in this <see cref="CollationKey"/>. In the case of an <see cref="Exception"/>, 
		/// information about the maximum strength to use will be returned in the <see cref="Exception"/>. 
		/// The user can then call <see cref="GetBound"/> again with the appropriate strength.
		/// </exception>
		/// <seealso cref="CollationBoundMode"/>
		/// <seealso cref="CollationStrength"/>
		public CollationKey GetBound(CollationBoundMode boundType, CollationStrength noOfLevels) {
			// Scan the string until we skip enough of the key OR reach the end of 
			// the key
			int offset = 0;
			int keystrength = (int)CollationStrength.Primary;
        
			if (noOfLevels > CollationStrength.Primary) {
				while (offset < m_key_.Length && m_key_[offset] != 0) {
					if (m_key_[offset ++] == SORT_LEVEL_TERMINATOR_) {
						keystrength ++;
						noOfLevels --;
						if (noOfLevels == CollationStrength.Primary || 
							offset == m_key_.Length || m_key_[offset] == 0) {
							offset --;
							break;
						}
					}
				} 
			}
        
			if (noOfLevels > 0) {
				throw new ArgumentException(
					"Source collation key has only " 
					+ keystrength 
					+ " strength level. Call getBound() again "
					+ " with noOfLevels < " + keystrength);
			}
        
			// READ ME: this code assumes that the values for BoundMode variables 
			// will not changes. They are set so that the enum value corresponds to 
			// the number of extra bytes each bound type needs.
			byte[] resultkey = new byte[offset + (int)boundType + 1];
			Array.Copy(m_key_, 0, resultkey, 0, offset);
			switch (boundType) {
				case CollationBoundMode.Lower: // = 0
					// Lower bound just gets terminated. No extra bytes
					break;
				case CollationBoundMode.Upper: // = 1
					// Upper bound needs one extra byte
					resultkey[offset ++] = 2;
					break;
				case CollationBoundMode.UpperLong: // = 2
					// Upper long bound needs two extra bytes
					resultkey[offset ++] = (byte)0xFF;
					resultkey[offset ++] = (byte)0xFF;
					break;
				default:
					throw new ArgumentException("Illegal boundType argument");
			}
			resultkey[offset ++] = 0;
			return new CollationKey(null, resultkey);
		}

		/// <summary>
		/// Merges this CollationKey with another.
		/// </summary>
		/// <param name="source">The <see cref="CollationKey"/> to merge with.</param>
		/// <remarks>
		/// Only the sorting order of the <see cref="CollationKey"/> will be merged. This method 
		/// does not attempt to merge the string representations of the <see cref="CollationKey"/>, 
		/// hence null will be returned as the string representation.
		/// <para>
		/// The strength levels are merged with their corresponding counterparts 
		/// (PRIMARIES with PRIMARIES, SECONDARIES with SECONDARIES etc.). 
		/// </para>
		/// <para>
		/// The merged string representation of the result <see cref="CollationKey"/> will be 
		/// a concatenation of the String representations of the 2 source <see cref="CollationKey"/>.
		/// </para>
		/// <para>
		/// Between the values from the same level a separator is inserted.
		/// 
		/// example (uncompressed):
		/// <code>
		/// 191B1D 01 050505 01 910505 00 and 1F2123 01 050505 01 910505 00
		/// will be merged as 
		/// 191B1D 02 1F212301 050505 02 050505 01 910505 02 910505 00
		/// </code>
		/// </para>
		/// <para>
		/// This allows for concatenating of first and last names for sorting, among 
		/// other things.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="CollationKey"/> that contains the valid merged sorting order 
		/// with a null string representation.
		/// </returns>
		/// <exception cref="ArgumentException">
		/// Thrown if source <see cref="CollationKey"/> argument is null or of 0 length.
		/// </exception>
		public CollationKey Merge(CollationKey source) {
			// check arguments
			if (source == null || source.GetLength() == 0) {
				throw new ArgumentException("CollationKey argument can not be null or of 0 length");
			}
    
			GetLength(); // gets the length of this sort key
			int sourcelength = source.GetLength();
			// 1 extra for the last strength that has no seperators
			byte[] result = new byte[m_length_ + sourcelength + 2];
    
			// merge the sort keys with the same number of levels
			int rindex = 0;
			int index = 0;
			int sourceindex = 0;
			while (true) { 
				// while both have another level
				// copy level from src1 not including 00 or 01
				// unsigned issues
				while (m_key_[index] < 0 || m_key_[index] >= MERGE_SEPERATOR_) {
					result[rindex ++] = m_key_[index ++];
				}
    
				// add a 02 merge separator
				result[rindex ++] = (byte)MERGE_SEPERATOR_;
    
				// copy level from src2 not including 00 or 01
				while (source.m_key_[sourceindex] < 0 
					|| source.m_key_[sourceindex] >= MERGE_SEPERATOR_) {
					result[rindex ++] = source.m_key_[sourceindex ++];
				}
    
				// if both sort keys have another level, then add a 01 level 
				// separator and continue
				if (m_key_[index] == SORT_LEVEL_TERMINATOR_ && 
					source.m_key_[sourceindex] == SORT_LEVEL_TERMINATOR_) {
					++ index;
					++ sourceindex;
					result[rindex ++] = SORT_LEVEL_TERMINATOR_;
				}
				else {
					break;
				}
			}
    
			// here, at least one sort key is finished now, but the other one
			// might have some contents left from containing more levels;
			// that contents is just appended to the result
			if (m_key_[index] != 0) {
				Array.Copy(m_key_, index, result, rindex, m_length_ - index);
			}
			else if (source.m_key_[sourceindex] != 0) {
				Array.Copy(source.m_key_, sourceindex, result, rindex, source.m_length_ - sourceindex);
			}
			result[result.Length - 1] = 0;
    
			// trust that neither sort key contained illegally embedded zero bytes
			return new CollationKey(null, result);
		}
		#endregion
	}
}