using System;
using System.Collections.Generic;
using System.Linq;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
    public sealed class SqlCompileResult {
        public SqlCompileResult(SqlCompileContext compileContext) {
            if (compileContext == null)
                throw new ArgumentNullException("compileContext");

            CompileContext = compileContext;
			Messages = new List<SqlCompileMessage>();
			Statements = new List<SqlStatement>();
        }

        public SqlCompileContext CompileContext { get; private set; }

		public ICollection<SqlCompileMessage> Messages { get; private set; }

	    public bool HasErrors {
		    get { return Messages.Any(x => x.Level == CompileMessageLevel.Error); }
	    }

	    public ICollection<SqlStatement> Statements { get; private set; }
    }
}