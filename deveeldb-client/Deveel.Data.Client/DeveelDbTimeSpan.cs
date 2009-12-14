using System;
using System.Data.SqlTypes;

namespace Deveel.Data.Client {
	public struct DeveelDbTimeSpan : IComparable, INullable {
		private DeveelDbTimeSpan(TimeSpan value, bool isNull) {
			this.value = value;
			this.isNull = isNull;
		}

		public DeveelDbTimeSpan(TimeSpan value)
			: this(value, false) {
		}

		public DeveelDbTimeSpan(long ticks)
			: this(new TimeSpan(ticks)) {
		}

		public DeveelDbTimeSpan(int hours, int minutes, int seconds)
			: this(new TimeSpan(hours, minutes, seconds)) {
		}

		public DeveelDbTimeSpan(int days, int hours, int minutes, int seconds)
			: this(new TimeSpan(days, hours, minutes, seconds)) {
		}

		public DeveelDbTimeSpan(int days, int hours, int minutes, int seconds, int milliseconds)
			: this(new TimeSpan(days, hours, minutes, seconds, milliseconds)) {
		}

		private readonly bool isNull;
		private readonly TimeSpan value;

		public static readonly DeveelDbTimeSpan Null = new DeveelDbTimeSpan(TimeSpan.Zero, true);
		public static readonly DeveelDbTimeSpan Zero = new DeveelDbTimeSpan(TimeSpan.Zero);

		#region Implementation of IComparable

		public TimeSpan Value {
			get {
				if (isNull)
					throw new InvalidOperationException();

				return value;
			}
		}

		public int Days {
			get { return value.Days; }
		}

		public int Hours {
			get { return Value.Hours; }
		}

		public int Minutes {
			get { return Value.Minutes; }
		}

		public int Seconds {
			get { return Value.Seconds; }
		}

		public int Milliseconds {
			get { return Value.Milliseconds; }
		}

		public int CompareTo(object obj) {
			if (obj == null || obj == DBNull.Value)
				return isNull ? 0 : -1;
			if (!(obj is DeveelDbTimeSpan))
				throw new ArgumentException();

			DeveelDbTimeSpan other = (DeveelDbTimeSpan) obj;
			if (isNull && other.isNull)
				return 0;
			if (isNull)
				return 1;
			if (other.isNull)
				return -1;

			return value.CompareTo(other.value);
		}

		public override bool Equals(object obj) {
			if (obj == null || obj == DBNull.Value)
				return isNull;

			if (!(obj is DeveelDbTimeSpan))
				return false;

			DeveelDbTimeSpan other = (DeveelDbTimeSpan) obj;
			if (isNull && other.isNull)
				return true;

			return value.Equals(other.value);
		}

		public override int GetHashCode() {
			return isNull ? 0 : value.GetHashCode();
		}

		public override string ToString() {
			return (isNull ? "NULL" : value.ToString());
		}

		#endregion

		#region Implementation of INullable

		public bool IsNull {
			get { return isNull; }
		}

		#endregion

		public static DeveelDbTimeSpan Parse(string s) {
			return new DeveelDbTimeSpan(TimeSpan.Parse(s));
		}

		public static DeveelDbBoolean operator ==(DeveelDbTimeSpan a, DeveelDbTimeSpan b) {
			if (a.isNull || b.isNull)
				return DeveelDbBoolean.Null;
			return a.Equals(b);
		}

		public static DeveelDbBoolean operator !=(DeveelDbTimeSpan a, DeveelDbTimeSpan b) {
			return !(a == b);
		}

		public static DeveelDbBoolean operator >(DeveelDbTimeSpan a, DeveelDbTimeSpan b) {
			if (a.isNull || b.isNull)
				return DeveelDbBoolean.Null;
			return a.CompareTo(b) < 0;
		}

		public static DeveelDbBoolean operator <(DeveelDbTimeSpan a, DeveelDbTimeSpan b) {
			if (a.isNull || b.isNull)
				return DeveelDbBoolean.Null;
			return a.CompareTo(b) > 0;
		}

		public static explicit operator DeveelDbTimeSpan(TimeSpan value) {
			return new DeveelDbTimeSpan(value);
		}

		public static explicit operator TimeSpan(DeveelDbTimeSpan value) {
			return value.Value;
		}
	}
}