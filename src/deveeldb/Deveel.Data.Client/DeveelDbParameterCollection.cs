using System;
using System.Collections;
using System.Data.Common;

using Deveel.Data.Sql;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameterCollection : DbParameterCollection {
		internal DeveelDbParameterCollection(DeveelDbCommand command) {
			if (command == null)
				throw new ArgumentNullException("command");

			Command = command;
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
			throw new NotImplementedException();
		}

		public override bool Contains(object value) {
			throw new NotImplementedException();
		}

		public override void Clear() {
			throw new NotImplementedException();
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
			get { throw new NotImplementedException(); }
		}

		public override object SyncRoot {
			get { throw new NotImplementedException(); }
		}

		public override bool IsFixedSize {
			get { throw new NotImplementedException(); }
		}

		public override bool IsReadOnly {
			get { throw new NotImplementedException(); }
		}

		public override bool IsSynchronized {
			get { throw new NotImplementedException(); }
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
