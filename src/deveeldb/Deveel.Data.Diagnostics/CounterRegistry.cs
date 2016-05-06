// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Diagnostics {
	public class CounterRegistry : ICounterRegistry, IDisposable {
		private Dictionary<string, Counter> counters;
		private readonly object countLock = new object();

		public CounterRegistry() {
			counters = new Dictionary<string, Counter>();
		}

		public IEnumerator<Counter> GetEnumerator() {
			lock (countLock) {
				return counters.Values.GetEnumerator();
			}
		}

		~CounterRegistry() {
			Dispose(false);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
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
				counters[key] = new Counter(key, value);
			}
		}

		internal void Increment(string key) {
			lock (countLock) {
				Counter counter;
				if (!counters.TryGetValue(key, out counter))
					counters[key] = counter = new Counter(key, null);

				counter.Increment();
			}
		}

		public bool TryCount(string name, out Counter counter) {
			lock (countLock) {
				return counters.TryGetValue(name, out counter);
			}
		}
	}
}
