using System;
using System.Collections.Generic;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
    public interface ISqlCompiler {
        SqlCompileResult Compile(SqlCompileContext context);
    }
}