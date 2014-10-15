// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Types;

namespace Deveel.Data {
	[Serializable]
	public abstract class DataObject : IComparable, IComparable<DataObject> {
		protected DataObject(DataType type) {
			if (type == null)
				throw new ArgumentNullException("type");

			Type = type;
		}

		public DataType Type { get; private set; }

		public virtual bool IsNull {
			get { return false; }
		}

		public bool IsComparableTo(DataObject obj) {
			return Type.IsComparable(obj.Type);
		}

		public virtual int CompareTo(DataObject other) {
			if (!IsComparableTo(other))
				throw new NotSupportedException();

			return Type.Compare(this, other);
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is StringObject))
				throw new ArgumentException();

			var other = obj as StringObject;
			return CompareTo(other);
		}

		/// <summary>
		/// Compares to the given object to verify if is it compatible.
		/// </summary>
		/// <param name="other">The other object to verify.</param>
		/// <returns>
		/// Returns an instance of <see cref="BooleanObject"/> that defines
		/// if the given object is compatible with the current one.
		/// </returns>
		/// <seealso cref="IsComparableTo"/>
		/// <seealso cref="DataType.IsComparable"/>
		public BooleanObject Is(DataObject other) {
			if (IsNull && other.IsNull)
				return BooleanObject.True;
			if (IsComparableTo(other))
				return Boolean(CompareTo(other) == 0);

			return BooleanObject.False;
		}

		/// <summary>
		/// Compares to the given object to verify if is it equal to the current.
		/// </summary>
		/// <param name="other">The other object to verify.</param>
		/// <remarks>
		/// This method returns a boolean value of <c>true</c> or <c>false</c>
		/// only if the current object and the other object are not <c>null</c>.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="BooleanObject"/> that defines
		/// if the given object is equal to the current one, or a boolean
		/// <c>null</c> if it was impossible to determine the types.
		/// </returns>
		/// <seealso cref="IsComparableTo"/>
		/// <seealso cref="DataType.IsComparable"/>
		public BooleanObject IsEqualTo(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) == 0);

			return Boolean(null);
		}

		public BooleanObject IsNotEqualTo(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) != 0);

			return Boolean(null);
		}

		public BooleanObject IsGreaterThan(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) < 0);

			return Boolean(null);			
		}

		public BooleanObject IsSmallerThan(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) > 0);

			return Boolean(null);
		}

		public BooleanObject IsGreterOrEqualThan(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) <= 0);

			return Boolean(null);
		}

		public BooleanObject IsSmallerOrEqualThan(DataObject other) {
			if (IsComparableTo(other) && !IsNull && !other.IsNull)
				return Boolean(CompareTo(other) >= 0);

			return Boolean(null);
		}

		public int SizeOf() {
			return Type.SizeOf(this);
		}

		#region Object Factory

		public static BooleanObject Boolean(bool? value) {
			return new BooleanObject(PrimitiveTypes.Boolean(), value);
		}

		public static StringObject String(string s) {
			return new StringObject(PrimitiveTypes.String(SqlTypeCode.String), s);
		}

		public static StringObject VarChar(string s) {
			return new StringObject(PrimitiveTypes.String(SqlTypeCode.VarChar), s);
		}

		#endregion
	}
}