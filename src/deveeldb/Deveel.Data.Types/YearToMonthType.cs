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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class YearToMonthType : DataType {
		public YearToMonthType()
			: this(2) {
		}

		public YearToMonthType(byte yearPrecision)
			: base("YEAR TO MONTH", SqlTypeCode.YearToMonth) {
			if (yearPrecision > 9)
				throw new ArgumentOutOfRangeException("yearPrecision", "YEAR precision can be maximum 9.");

			YearPrecision = yearPrecision;
		}

		public byte YearPrecision { get; private set; }

		public override bool IsIndexable {
			get { return false; }
		}

		public override bool CanCastTo(DataType type) {
			return base.CanCastTo(type);
		}

		public override DataObject CastTo(DataObject value, DataType destType) {
			return base.CastTo(value, destType);
		}

		public override bool Equals(DataType other) {
			var otherType = other as YearToMonthType;
			if (otherType == null)
				return false;

			return YearPrecision == otherType.YearPrecision;
		}

		public override DataType Wider(DataType otherType) {
			var other = otherType as YearToMonthType;
			if (other == null)
				throw new ArgumentException("The other type is not YEAR TO MONTH span");

			if (YearPrecision >= other.YearPrecision)
				return this;

			return YearPrecision < other.YearPrecision ? other : base.Wider(otherType);
		}

		public override int Compare(ISqlObject x, ISqlObject y) {
			if (x is SqlYearToMonth &&
			    y is SqlYearToMonth) {
				var a = (SqlYearToMonth) x;
				var b = (SqlYearToMonth) y;

				return x.CompareTo(b);
			}

			return base.Compare(x, y);
		}
	}
}