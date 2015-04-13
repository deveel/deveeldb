// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Objects {
	/// <summary>
	/// An object that provides methods for accessing a
	/// finite collection of SQL expressions.
	/// </summary>
	[Serializable]
	public sealed class SqlArray : ISqlObject, IEnumerable<SqlExpression> {
		private readonly SqlExpression[] expressions;

		/// <summary>
		/// A SQL array that is equivalent to <c>null</c>.
		/// </summary>
		public static readonly SqlArray Null = new SqlArray(null);

		/// <summary>
		/// Constructs a new <see cref="SqlArray"/> on the given array
		/// of expressions.
		/// </summary>
		/// <param name="expressions">The array of <see cref="SqlExpression"/> that
		/// is the source of this array.</param>
		public SqlArray(SqlExpression[] expressions) {
			if (expressions == null) {
				this.expressions = null;
				IsNull = true;
			} else {
				this.expressions = new SqlExpression[expressions.Length];
				Array.Copy(expressions, this.expressions, expressions.Length);
			}
		}

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		public bool IsNull { get; private set; }

		/// <summary>
		/// Gets the length of the array.
		/// </summary>
		/// <exception cref="NullReferenceException">
		/// If the array is <c>null</c>.
		/// </exception>
		public int Length {
			get {
				AssertNotNull();
				return expressions.Length;
			}
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		private void AssertNotNull() {
			if (IsNull)
				throw new NullReferenceException("The array is null");
		}

		/// <summary>
		/// Gets the expression at the given index of the array.
		/// </summary>
		/// <param name="index">The zero-based index in the array at 
		/// which to get the expression</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlExpression"/> at the index
		/// given within the array.
		/// </returns>
		/// <exception cref="NullReferenceException">
		/// If the array is <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="index"/> is lower than zero
		/// or greater or equal than <see cref="Length"/>.
		/// </exception>
		/// <seealso cref="GetValue"/>
		public SqlExpression this[int index] {
			get { return GetValue(index); }
		}

		/// <summary>
		/// Gets the expression at the given index of the array.
		/// </summary>
		/// <param name="index">The zero-based index in the array at 
		/// which to get the expression</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlExpression"/> at the index
		/// given within the array.
		/// </returns>
		/// <exception cref="NullReferenceException">
		/// If the array is <c>null</c>.
		/// </exception>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="index"/> is lower than zero
		/// or greater or equal than <see cref="Length"/>.
		/// </exception>
		/// <seealso cref="GetValue"/>
		public SqlExpression GetValue(int index) {
			AssertNotNull();

			if (index < 0 || index >= Length)
				throw new ArgumentOutOfRangeException("index");

			return expressions[index];
		}

		public IEnumerator<SqlExpression> GetEnumerator() {
			AssertNotNull();
			return new Enumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#region Enumerator

		class Enumerator : IEnumerator<SqlExpression> {
			private int index;
			private int length;
			private readonly SqlArray array;

			public Enumerator(SqlArray array) {
				this.array = array;
				length = array.Length;
				index = -1;
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				array.AssertNotNull();
				return ++index < length;
			}

			public void Reset() {
				array.AssertNotNull();

				index = -1;
				length = array.Length;
			}

			public SqlExpression Current {
				get {
					array.AssertNotNull();
					return array.GetValue(index);
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}