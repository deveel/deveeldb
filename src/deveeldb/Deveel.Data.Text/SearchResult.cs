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
using System.Collections;

namespace Deveel.Data.Text {
	public sealed class SearchResult {
		#region .ctor
		public SearchResult(string query, int capacity) {
			this.query = query;
			results = new Hashtable(capacity);
		}
		#endregion
		
		#region Fields
		private string query;
		private Hashtable results;
		#endregion

		#region Properties
		public string Query {
			get { return query; }
		}
		#endregion

		#region Public Methods
		public void AddScore(int rowIndex, double score) {
			results.Add(rowIndex, score);
		}
		
		public double GetScore(int rowIndex) {
			return (double)results[rowIndex];
		}
		#endregion
	}
}