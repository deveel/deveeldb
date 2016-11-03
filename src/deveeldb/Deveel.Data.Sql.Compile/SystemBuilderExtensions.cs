// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Build;

namespace Deveel.Data.Sql.Compile {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseSqlCompiler(this ISystemBuilder builder, ISqlCompiler compiler) {
			return builder.Use<ISqlCompiler>(options => options.With(compiler).InSystemScope());
		}

		public static ISystemBuilder UseSqlCompiler<T>(this ISystemBuilder builder) where T : class, ISqlCompiler {
			return builder.Use<ISqlCompiler>(options => options.With<T>().InSystemScope());
		}

		public static ISystemBuilder UseDefaultSqlCompiler(this ISystemBuilder builder) {
			return builder.UseSqlCompiler<PlSqlCompiler>();			
		}
	}
}
