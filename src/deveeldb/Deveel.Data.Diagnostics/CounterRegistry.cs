using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Deveel.Data.Diagnostics {
	public class CounterRegistry : IDisposable {
		private Dictionary<string, object> counters;
		private readonly object countLock = new object();

		public CounterRegistry() {
			counters = new Dictionary<string, object>();
		}

		~CounterRegistry() {
			Dispose(false);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				lock (countLock) {
					if (counters != null)
						counters.Clear();
				}
			}

			lock (countLock) {
				counters = null;
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		internal void SetValue(string key, object value) {
			lock (countLock) {
				counters[key] = value;
			}
		}

		internal void Increment(string key) {
			lock (countLock) {
				object value;
				if (!counters.TryGetValue(key, out value)) {
					value = 1L;
				} else {
					if (value is long) {
						value = ((long) value) + 1;
					} else if (value is int) {
						value = (int) value + 1;
					} else if (value is double) {
						value = (double) value + 1;
					} else {
						throw new InvalidOperationException(String.Format("The value for '{0}' is not a numeric.", key));
					}
				}

				counters[key] = value;
			}
		}

		public T GetCount<T>(string key) {
			T value;
			if (!TryGetCount(key, out value))
				return default(T);

			return value;
		}

		public bool TryGetCount<T>(string key, out T value) {
			lock (countLock) {
				object obj;
				if (!counters.TryGetValue(key, out obj)) {
					value = default(T);
					return false;
				}

				if (obj == null) {
					value = default(T);
				} else if (!(obj is T)) {
					value = (T) Convert.ChangeType(obj, typeof(T), CultureInfo.InvariantCulture);
				} else {
					value = (T) obj;
				}


				return true;
			}
		}

		public IDictionary<string, object> Counters {
			get {
				lock (countLock) {
					return counters.ToDictionary(x => x.Key, y => y.Value);
				}
			}
		}
	}
}
