﻿// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Compile {
    public sealed class SqlCompileContext {
	    public SqlCompileContext(string sourceText) 
			: this(null, sourceText) {
	    }

	    public SqlCompileContext(IContext context, string sourceText) {
            if (string.IsNullOrEmpty(sourceText))
                throw new ArgumentNullException("sourceText");

            Context = context;
            SourceText = sourceText;
        }

        public IContext Context { get; private set; }

	    public bool IsInContext {
			get { return Context != null; }
	    }

        public string SourceText { get; private set; }
    }
}