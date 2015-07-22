using System;
using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Compile {
    public sealed class SqlCompileContext {
        internal SqlCompileContext(ISystemContext systemContext, string sourceText) {
            if (systemContext == null)
                throw new ArgumentNullException("systemContext");
            if (string.IsNullOrEmpty(sourceText))
                throw new ArgumentNullException("sourceText");

            SystemContext = systemContext;
            SourceText = sourceText;
        }

        public ISystemContext SystemContext { get; private set; }

        public string SourceText { get; private set; }
    }
}