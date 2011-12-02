using System;
using System.Collections;
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameterCollection : DbParameterCollection {
		internal DeveelDbParameterCollection(DeveelDbCommand command) {
			this.command = command;

			parameters = new ArrayList();
			paramNameCache = new Hashtable(StringComparer.CurrentCultureIgnoreCase);
		}

		private readonly DeveelDbCommand command;
		private readonly ArrayList parameters;
		private readonly Hashtable paramNameCache;

		internal DeveelDbCommand Command {
			get { return command; }
		}

		public new DeveelDbParameter this[int index] {
			get { return (DeveelDbParameter) GetParameter(index); }
			set { SetParameter(index, value); }
		}

		public new DeveelDbParameter this[string parameterName] {
			get { return (DeveelDbParameter) GetParameter(parameterName); }
			set { SetParameter(parameterName, value); }
		}

		private void CheckParamIndex(int index) {
			if (index < 0 || index >= parameters.Count)
				throw new ArgumentOutOfRangeException();
		}

		private void CheckNamedStyle() {
			if (command.Connection.Settings.ParameterStyle != ParameterStyle.Named)
				throw new ArgumentException();
		}

		private static void CheckParameterType(object value) {
			if (!(value is DeveelDbParameter))
				throw new ArgumentException();
		}

		public DeveelDbParameter Add(DeveelDbParameter parameter) {
			if (command.Connection.Settings.ParameterStyle == ParameterStyle.Named &&
			    Contains(parameter.ParameterName))
				throw new ArgumentException("The parameter '" + parameter.ParameterName + "' was already set.");

			parameter.Collection = this;
			parameters.Add(parameter);
			return parameter;
		}

		public DeveelDbParameter Add(string parameterName, DeveelDbType type, int size) {
			return Add(new DeveelDbParameter(parameterName, type, size));
		}

		public DeveelDbParameter Add(string paramaterName, DeveelDbType type) {
			return Add(new DeveelDbParameter(paramaterName, type));
		}

		public DeveelDbParameter Add(string parameterName, object value) {
			return Add((new DeveelDbParameter(parameterName, value)));
		}

		#region Overrides of DbParameterCollection

		public override int Add(object value) {
			ParameterStyle parameterStyle = command.Connection.Settings.ParameterStyle;
			if (!(value is DeveelDbParameter)) {
				if (parameterStyle != ParameterStyle.Marker)
					throw new ArgumentException();

				value = new DeveelDbParameter(value);
			}

			DeveelDbParameter parameter = (DeveelDbParameter) value;
			if (parameterStyle == ParameterStyle.Named &&
				Contains(parameter.ParameterName))
				throw new ArgumentException("The parameter '" + parameter.ParameterName + "' was already set.");

			parameter.Collection = this;
			return parameters.Add(parameter);
		}

		public override bool Contains(object value) {
			CheckParameterType(value);

			if (command.Connection.Settings.ParameterStyle != ParameterStyle.Named)
				return false;

			DeveelDbParameter parameter = (DeveelDbParameter)value;
			return Contains(parameter.ParameterName);
		}

		public override void Clear() {
			parameters.Clear();
			paramNameCache.Clear();
		}

		public override int IndexOf(object value) {
			CheckParameterType(value);

			if (command.Connection.Settings.ParameterStyle != ParameterStyle.Named)
				return -1;

			return IndexOf(((DeveelDbParameter) value).ParameterName);
		}

		public override void Insert(int index, object value) {
			ParameterStyle parameterStyle = command.Connection.Settings.ParameterStyle;
			if (!(value is DeveelDbParameter)) {
				if (parameterStyle != ParameterStyle.Marker)
					throw new ArgumentException();

				value = new DeveelDbParameter(value);
			}

			DeveelDbParameter parameter = (DeveelDbParameter)value;
			if (parameterStyle == ParameterStyle.Named &&
				Contains(parameter.ParameterName))
				throw new ArgumentException("The parameter '" + parameter.ParameterName + "' was already set.");

			parameter.Collection = this;
			parameters.Insert(index, value);
		}

		public override void Remove(object value) {
			CheckParameterType(value);
			CheckNamedStyle();

			DeveelDbParameter parameter = (DeveelDbParameter) value;
			int index = IndexOf(parameter.ParameterName);
			if (index == -1)
				throw new ArgumentException();
				
			RemoveAt(index);
		}

		public override void RemoveAt(int index) {
			CheckParamIndex(index);

			parameters.RemoveAt(index);
			paramNameCache.Clear();
		}

		public override void RemoveAt(string parameterName) {
			CheckNamedStyle();

			int index = IndexOf(parameterName);
			if (index == -1)
				throw new InvalidOperationException();

			RemoveAt(index);
		}

		protected override void SetParameter(int index, DbParameter value) {
			CheckParamIndex(index);
			CheckParameterType(value);

			parameters[index] = value;
			paramNameCache.Clear();
		}

		protected override void SetParameter(string parameterName, DbParameter value) {
			CheckNamedStyle();

			int index = IndexOf(parameterName);
			if (index == -1)
				throw new ArgumentException();

			SetParameter(index, value);
		}

		public override int Count {
			get { return parameters.Count; }
		}

		public override object SyncRoot {
			get { return parameters.SyncRoot; }
		}

		public override bool IsFixedSize {
			get { return false; }
		}

		public override bool IsReadOnly {
			get { return parameters.IsReadOnly; }
		}

		public override bool IsSynchronized {
			get { return parameters.IsSynchronized; }
		}

		public override int IndexOf(string parameterName) {
			CheckNamedStyle();

			if (parameterName == null || parameterName.Length == 0)
				throw new ArgumentNullException("parameterName");

			if (parameterName[0] == '@')
				parameterName = parameterName.Substring(1);

			object index = paramNameCache[parameterName];
			if (index == null) {
				for (int i = 0; i < parameters.Count; i++) {
					DeveelDbParameter parameter = (DeveelDbParameter) parameters[i];
					if (parameter.ParameterName == parameterName) {
						index = i;
						break;
					}
				}

				if (index != null)
					paramNameCache[parameterName] = index;
			}

			return index == null ? -1 : (int) index;
		}

		public override IEnumerator GetEnumerator() {
			return parameters.GetEnumerator();
		}

		protected override DbParameter GetParameter(int index) {
			CheckParamIndex(index);
			return parameters[index] as DeveelDbParameter;
		}

		protected override DbParameter GetParameter(string parameterName) {
			CheckNamedStyle();

			int index = IndexOf(parameterName);
			if (index == -1)
				return null;

			return GetParameter(index);
		}

		public override bool Contains(string value) {
			CheckParameterType(value);

			return IndexOf(value) != -1;
		}

		public override void CopyTo(Array array, int index) {
			parameters.CopyTo(array, index);
		}

		public override void AddRange(Array values) {
			for (int i = 0; i < values.Length; i++)
				Add(values.GetValue(i));
		}

		#endregion

		internal void OnParameterNameChanged(string oldName, string newName) {
			int index = IndexOf(oldName);
			paramNameCache.Remove(oldName);
			paramNameCache[newName] = index;
		}
	}
}