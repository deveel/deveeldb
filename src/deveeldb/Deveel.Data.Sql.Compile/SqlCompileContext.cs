using System;
using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Compile {
    public sealed class SqlCompileContext {
	    internal SqlCompileContext(string sourceText) 
			: this(null, sourceText) {
	    }

	    internal SqlCompileContext(ISystemContext systemContext, string sourceText) {
            if (string.IsNullOrEmpty(sourceText))
                throw new ArgumentNullException("sourceText");

            SystemContext = systemContext;
            SourceText = sourceText;
        }

        public ISystemContext SystemContext { get; private set; }

	    public bool IsInContext {
			get { return SystemContext != null; }
	    }

        public string SourceText { get; private set; }
    }
}