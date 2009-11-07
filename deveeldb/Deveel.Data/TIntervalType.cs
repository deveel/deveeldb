using System;

using Deveel.Math;

namespace Deveel.Data {
	public sealed class TIntervalType : TType {
		public TIntervalType(SqlType type, int length)
			: base(type) {
			this.length = length;
		}

		private readonly int length;

		public int Length {
			get { return length; }
		}

		public override int Compare(object x, object y) {
			TimeSpan a = (TimeSpan) x;
			TimeSpan b = TimeSpan.Zero;
			if (y is TimeSpan) {
				b = (TimeSpan) y;
			} else if (y is BigNumber) {
				int num = ((BigNumber) y).ToInt32();
				if (SQLType == SqlType.Year) {
					//TODO: if the number is smaller than 4 numbers, calculate it...
					b = new TimeSpan(num*(TimeSpan.TicksPerDay*365));
				} else if (SQLType == SqlType.Month)
					// month range is 0..11
					b = new TimeSpan((num + 1) * ((TimeSpan.TicksPerDay * 365) / 12));
				else if (SQLType == SqlType.Day)
					b = new TimeSpan(num * TimeSpan.TicksPerDay);
				else if (SQLType == SqlType.Day)
					b = new TimeSpan(num * TimeSpan.TicksPerMinute);
				else if (SQLType == SqlType.Second)
					b = new TimeSpan(num * TimeSpan.TicksPerSecond);
			} else {
				throw new ArgumentException();
			}

			return a.CompareTo(b);
		}

		public override bool IsComparableType(TType type) {
			return (type is TIntervalType || type is TNumericType);
		}

		public override int CalculateApproximateMemoryUse(object ob) {
			return 4 + 8;
		}

		public override Type GetObjectType() {
			return typeof (TimeSpan);
		}
	}
}