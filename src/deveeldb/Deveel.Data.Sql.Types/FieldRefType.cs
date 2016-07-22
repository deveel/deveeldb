using System;

namespace Deveel.Data.Sql.Types {
	public sealed class FieldRefType : SqlType {
		public FieldRefType(ObjectName fieldName)
			: base(String.Format("{0}%TYPE", fieldName), SqlTypeCode.FieldRef) {
			if (fieldName == null)
				throw new ArgumentNullException("fieldName");

			FieldName = fieldName;
		}

		public ObjectName FieldName { get; private set; }

		public override bool IsReference {
			get { return true; }
		}

		public override SqlType Resolve(IRequest context) {
			return context.Access().ResolveFieldType(FieldName);
		}
	}
}
