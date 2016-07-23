using System;

namespace Deveel.Data.Sql.Types {
	public sealed class RowRefType : SqlType {
		public RowRefType(ObjectName objectName)
			: base(String.Format("{0}%ROWTYPE", objectName), SqlTypeCode.RowRef) {
			if (objectName == null)
				throw new ArgumentNullException("objectName");

			ObjectName = objectName;
		}

		public ObjectName ObjectName { get; private set; }

		public override bool IsReference {
			get { return true; }
		}

		public override SqlType Resolve(IRequest context) {
			return context.Access().ResolveRowType(ObjectName);
		}
	}
}
