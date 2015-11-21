using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Index {
 	public sealed class LuceneIndex : ColumnIndex {
 		public LuceneIndex(ITable table, int columnOffset) 
			: base(table, columnOffset) {
 		}

 		public override string IndexType {
 			get { throw new NotImplementedException(); }
 		}

 		public override ColumnIndex Copy(ITable table, bool readOnly) {
 			throw new NotImplementedException();
 		}

 		public override void Insert(int rowNumber) {
 			throw new NotImplementedException();
 		}

 		public override void Remove(int rowNumber) {
 			throw new NotImplementedException();
 		}

 		public override IEnumerable<int> SelectRange(IndexRange[] ranges) {
 			throw new NotImplementedException();
 		}
 	}
}
