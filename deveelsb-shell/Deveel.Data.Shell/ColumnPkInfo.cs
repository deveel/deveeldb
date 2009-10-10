using System;

namespace Deveel.Data.Shell {
	public sealed class ColumnPkInfo {

		private readonly String _pkName;
		private readonly int _columnIndex;

		public ColumnPkInfo(String pkName, int columnIndex) {
			_pkName = pkName;
			_columnIndex = columnIndex;
		}

		public int getColumnIndex() {
			return _columnIndex;
		}

		public String getPkName() {
			return _pkName;
		}

		public override bool Equals(Object other) {
			bool result = false;
			if (other != null && other is ColumnPkInfo) {
				ColumnPkInfo o = (ColumnPkInfo)other;
				if (_pkName != null && _pkName.Equals(o.getPkName())
					 || _pkName == null && o.getPkName() == null) {
					result = true;
				}
			}
			return result;
		}
	}
}