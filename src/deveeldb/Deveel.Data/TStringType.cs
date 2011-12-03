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
using System.Globalization;
using System.IO;

using Deveel.Data.Text;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="TType"/> for a <see cref="String"/>.
	/// </summary>
	[Serializable]
	public sealed class TStringType : TType {
		/// <summary>
		/// The maximum allowed size for the string.
		/// </summary>
		private readonly int max_size;

		/// <summary>
		/// The locale of the string.
		/// </summary>
		private readonly CultureInfo locale;

		/// <summary>
		/// The strength of the collator for this string.
		/// </summary>
		private readonly CollationStrength strength;
		/// <summary>
		/// The decomposition mode of the collator for this string type.
		/// </summary>
		private readonly CollationDecomposition decomposition;

		/// <summary>
		/// The collator object for this type, created when we first compare objects.
		/// </summary>
		private CompareInfo collator;

		/// <summary>
		/// Constructs a type with the given <see cref="SqlType"/> value, the 
		/// maximum size, and the <see cref="CultureInfo">locale</see> string of 
		/// the type.
		/// </summary>
		/// <param name="sql_type">The string <see cref="SqlType"/> of the type.</param>
		/// <param name="max_size">The maximum size of the string value.</param>
		/// <param name="locale">The <see cref="CultureInfo">locale</see> of the string 
		/// type used to collate the strings.</param>
		/// <param name="strength"></param>
		/// <param name="decomposition"></param>
		public TStringType(SqlType sql_type, int max_size, CultureInfo locale, CollationStrength strength, CollationDecomposition decomposition)
			: base(sql_type) {
			this.max_size = max_size;
			this.strength = strength;
			this.decomposition = decomposition;
			this.locale = locale;
		}

		/// <summary>
		/// Constructs a type with the given <see cref="SqlType"/> value, the 
		/// maximum size, and the locale string of the type.
		/// </summary>
		/// <param name="sql_type">The string <see cref="SqlType"/> of the type.</param>
		/// <param name="max_size">The maximum size of the string value.</param>
		/// <param name="locale_str">The string representation of the string locale.</param>
		/// <param name="strength"></param>
		/// <param name="decomposition"></param>
		/// <remarks>
		/// This constructor will parse the given <paramref name="locale_str">locale 
		/// string</paramref>
		/// and call the right <see cref="CultureInfo"/>.
		/// </remarks>
		/// <seealso cref="TStringType(SqlType, Int32, CultureInfo, CollationStrength, CollationDecomposition"/>
		public TStringType(SqlType sql_type, int max_size, String locale_str,
						   CollationStrength strength, CollationDecomposition decomposition)
			: base(sql_type) {
			this.max_size = max_size;
			this.strength = strength;
			this.decomposition = decomposition;

			if (locale_str != null && locale_str.Length >= 2) {
				String language = locale_str.Substring(0, 2);
				String country = "";
				String variant = "";
				if (locale_str.Length > 2) {
					country = locale_str.Substring(2, 2);
					if (locale_str.Length > 4) {
						variant = locale_str.Substring(4);
					}
				}
				string culture_string = language + "-" + country;
				if (variant != null)
					culture_string += variant;
				locale = new CultureInfo(culture_string);
			}

		}

		/// <summary>
		/// Constructor without strength and decomposition that sets to default levels.
		/// </summary>
		/// <param name="sql_type"></param>
		/// <param name="max_size"></param>
		/// <param name="locale_str"></param>
		public TStringType(SqlType sql_type, int max_size, String locale_str)
			: this(sql_type, max_size, locale_str, CollationStrength.Identical, CollationDecomposition.None) {
		}


		/// <summary>
		/// Returns the maximum size of the string (-1 is don't care).
		/// </summary>
		public int MaximumSize {
			get { return max_size; }
		}

		/// <summary>
		/// Returns the strength of this string type.
		/// </summary>
		public CollationStrength Strength {
			get { return strength; }
		}

		/// <summary>
		/// Returns the decomposition of this string type.
		/// </summary>
		public CollationDecomposition Decomposition {
			get { return decomposition; }
		}

		/// <summary>
		/// Returns the locale of the string.
		/// </summary>
		public CultureInfo Locale {
			get { return locale; }
		}

		/// <summary>
		/// Gets the locale information as a formatted string.
		/// </summary>
		/// <remarks>
		/// Note that a string type may be constructed with a NULL locale which
		/// means strings are compared lexicographically.  The string locale is
		/// formated as [2 char language][2 char country][rest is variant].  For
		/// example, US english would be 'en-US', French would be 'fr' and Germany
		/// would be 'de-DE'.
		/// </remarks>
		public string LocaleString {
			get { return locale == null ? "" : locale.Name; }
		}

		/// <summary>
		/// An implementation of a lexicographical CompareTo operation on 
		/// a <see cref="IStringAccessor"/> object.
		/// </summary>
		/// <param name="str1"></param>
		/// <param name="str2"></param>
		/// <remarks>
		/// This uses the <see cref="TextReader"/> object to compare the strings over a 
		/// stream if the size is such that it is more efficient to do so.
		/// </remarks>
		/// <returns></returns>
		private static int LexicographicalOrder(IStringAccessor str1, IStringAccessor str2) {
			// If both strings are small use the 'toString' method to compare the
			// strings.  This saves the overhead of having to store very large string
			// objects in memory for all comparisons.
			long str1_size = str1.Length;
			long str2_size = str2.Length;
			if (str1_size < 32 * 1024 &&
				str2_size < 32 * 1024) {
				return str1.ToString().CompareTo(str2.ToString());
			}

			// The minimum size
			long size = System.Math.Min(str1_size, str2_size);
			TextReader r1 = str1.GetTextReader();
			TextReader r2 = str2.GetTextReader();
			try {
				try {
					while (size > 0) {
						int c1 = r1.Read();
						int c2 = r2.Read();
						if (c1 != c2) {
							return c1 - c2;
						}
						--size;
					}
					// They compare equally up to the limit, so now compare sizes,
					if (str1_size > str2_size) {
						// If str1 is larger
						return 1;
					} else if (str1_size < str2_size) {
						// If str1 is smaller
						return -1;
					}
					// Must be equal
					return 0;
				} finally {
					r1.Close();
					r2.Close();
				}
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}

		}

		//TODO: implement a text framework for efficient comparison...

		/// <summary>
		/// Returns the <see cref="CompareInfo"/> object for this string type.
		/// </summary>
		/// <remarks>
		/// This collator is used to compare strings of this locale.
		/// <para>
		/// This method is synchronized because a side effect of this method 
		/// is to store the collator object instance in a local variable.
		/// </para>
		/// </remarks>
		private CompareInfo Collator {
			get {
				lock (this) {
					if (collator != null) {
						return collator;
					} else {
						//TODO:
						collator = locale.CompareInfo;
						return collator;
					}
				}
			}
		}

		// ---------- Overwritten from TType ----------

		/// <inheritdoc/>
		/// <remarks>
		/// For strings, the locale must be the same for the types to be comparable.
		/// If the locale is not the same then they are not comparable. Note that
		/// strings with a locale of null can be compared with any other locale. So
		/// this will only return false if both types have different (but defined)
		/// locales.
		/// </remarks>
		public override bool IsComparableType(TType type) {
			// Are we comparing with another string type?
			if (type is TStringType) {
				TStringType s_type = (TStringType)type;
				// If either locale is null return true
				if (Locale == null || s_type.Locale == null) {
					return true;
				}
				// If the locales are the same return true
				return Locale.Equals(s_type.Locale);
			}
			return false;
		}

		public override int Compare(Object ob1, Object ob2) {
			if (ob1 == ob2) {
				return 0;
			}
			// If lexicographical ordering,
			if (locale == null) {
				return LexicographicalOrder((IStringAccessor)ob1, (IStringAccessor)ob2);
				//      return ob1.toString().compareTo(ob2.toString());
			} else {
				return Collator.Compare(ob1.ToString(), ob2.ToString());
			}
		}

		public override int CalculateApproximateMemoryUse(Object ob) {
			return ob != null ? (((IStringAccessor) ob).Length*2) + 24 : 32;
		}

		public override Type GetObjectType() {
			return typeof(IStringAccessor);
		}

	}
}