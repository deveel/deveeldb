using System;
using System.Globalization;

using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class QueryParameter {
		public QueryParameter(DataType dataType) 
			: this(Marker, dataType) {
		}

		public QueryParameter(string name, DataType dataType) {
			if (dataType == null)
				throw new ArgumentNullException("dataType");

			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			if (!String.Equals(name, Marker, StringComparison.Ordinal) &&
				name[0] != NamePrefix)
				throw new ArgumentException(String.Format("The parameter name '{0}' is invalid: must be '{1}' or starting with '{2}'", name, Marker, NamePrefix));

			Name = name;
			DataType = dataType;
			Direction = QueryParameterDirection.In;
		}

		public const char NamePrefix = ':';
		public const string Marker = "?";

		public string Name { get; private set; }

		public DataType DataType { get; private set; }

		public QueryParameterDirection Direction { get; set; }

		public object Value { get; set; }
	}
}