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
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Deveel.Data.Sql;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameterCollection : DbParameterCollection {
		private readonly List<DeveelDbParameter> parameters;

		internal DeveelDbParameterCollection(DeveelDbCommand command) {
			if (command == null)
				throw new ArgumentNullException("command");

			Command = command;
			parameters = new List<DeveelDbParameter>();
		}

		private DeveelDbCommand Command { get; set; }

		public new DeveelDbParameter this[int offset] {
			get { return(DeveelDbParameter) GetParameter(offset); }
			set { SetParameter(offset, value); }
		}

		public new DeveelDbParameter this[string name] {
			get { return (DeveelDbParameter) GetParameter(name); }
		}

		private QueryParameterStyle ParameterStyle {
			get { return Command.Connection.Settings.ParameterStyle; }
		}

		public override int Add(object value) {
			if (value is DeveelDbParameter)
				return AddParameter((DeveelDbParameter) value);
			if (value is IDbDataParameter)
				return AddDbDataParameter((IDbDataParameter) value);

			return AddValue(value);
		}

		private int AddValue(object value) {
			if (ParameterStyle != QueryParameterStyle.Marker)
				throw new ArgumentException("Cannot add an unnamed parameter in this context.");

			throw new NotImplementedException();
		}

		private int AddDbDataParameter(IDbDataParameter parameter) {
			// TODO:
			throw new NotImplementedException();
		}

		private int AddParameter(DeveelDbParameter parameter) {
			throw new NotImplementedException();
		}

		public override bool Contains(object value) {
			if (value is string)
				return Contains((string) value);
			if (value is IDbDataParameter)
				return Contains(((IDbDataParameter) value).ParameterName);

			return false;
		}

		public override void Clear() {
			parameters.Clear();
		}

		public override int IndexOf(object value) {
			throw new NotImplementedException();
		}

		public override void Insert(int index, object value) {
			throw new NotImplementedException();
		}

		public override void Remove(object value) {
			throw new NotImplementedException();
		}

		public override void RemoveAt(int index) {
			throw new NotImplementedException();
		}

		public override void RemoveAt(string parameterName) {
			throw new NotImplementedException();
		}

		protected override void SetParameter(int index, DbParameter value) {
			throw new NotImplementedException();
		}

		protected override void SetParameter(string parameterName, DbParameter value) {
			throw new NotImplementedException();
		}

		public override int Count {
			get { return parameters.Count; }
		}

		public override object SyncRoot {
			get { return null; }
		}

		public override bool IsFixedSize {
			get { return false; }
		}

		public override bool IsReadOnly {
			get { return false; }
		}

		public override bool IsSynchronized {
			get { return false; }
		}

		public override int IndexOf(string parameterName) {
			throw new NotImplementedException();
		}

		public override IEnumerator GetEnumerator() {
			throw new NotImplementedException();
		}

		protected override DbParameter GetParameter(int index) {
			throw new NotImplementedException();
		}

		protected override DbParameter GetParameter(string parameterName) {
			throw new NotImplementedException();
		}

		public override bool Contains(string value) {
			throw new NotImplementedException();
		}

		public override void CopyTo(Array array, int index) {
			throw new NotImplementedException();
		}

		public override void AddRange(Array values) {
			throw new NotImplementedException();
		}
	}
}
