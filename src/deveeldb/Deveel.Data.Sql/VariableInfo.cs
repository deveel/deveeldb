using System;

using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class VariableInfo : IObjectInfo {
		public VariableInfo(string variableName, DataType type, bool isConstant) {
			if (String.IsNullOrEmpty(variableName))
				throw new ArgumentNullException("variableName");
			if (type == null)
				throw new ArgumentNullException("type");

			VariableName = variableName;
			Type = type;
			IsConstant = isConstant;
		}

		public string VariableName { get; private set; }

		public DataType Type { get; private set; }

		public bool IsConstant { get; private set; }

		public bool IsNotNull { get; set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Variable; }
		}

		ObjectName IObjectInfo.FullName {
			get { return new ObjectName(VariableName); }
		}
	}
}