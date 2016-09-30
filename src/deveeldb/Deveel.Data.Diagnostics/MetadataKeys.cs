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

namespace Deveel.Data.Diagnostics {
	public static class MetadataKeys {
		public static class System {
			public const string Version = "system.version";

			public static class Environment {
				public const string OsPlatform = "system.environment.osPlatform";
				public const string OsVersion = "system.environment.osVersion";
				public const string RuntimeVersion = "system.environment.runtimeVersion";
				public const string ServicePack = "system.environment.servicePack";
				public const string MachineName = "system.environment.machineName";
				public const string HostName = "system.environment.hostName";
				public const string ProcessorCount = "system.environment.processorCount";
			}

			public static class Configuration {
				public const string KeyFormat = "system.config[{key}]";
			}
		}
		public static class Database {
			public const string Name = "database.name";
			public const string SessionCount = "database.sessions.count";
			public const string DataVersion = "database.dataVersion";

			public sealed class Configuration {
				public const string KeyFormat = "database.config[{key}]";
			}
		}

		public static class Session {
			public const string LastCommandText = "session.lastCommand.text";
			public const string LastCommandTime = "session.lastCommand.time";
			public const string LastCommandAffectedRows = "session.lastCommand.affectedRows";
			public const string StartTimeUtc = "session.startTimeUtc";
			public const string TimeZone = "session.timeZone";
			public const string UserName = "session.userName";
		}

		public static class Transaction {
			public const string CommitId = "transaction.commitId";
			public const string ReadOnly = "transaction.readOnly";
			public const string IgnoreIdentifiersCase = "transaction.ignoreIdCase";
			public const string Schema = "transaction.schema";
			public const string IsolationLevel = "transaction.isolationLevel";
		}

		public static class Query {
			public const string StartTime = "query.startTime";
			public const string SourceText = "query.source.text";
		}

		public static class Event {
			public static class Information {
				public const string Level = "info.level";
				public const string Message = "info.message";
			}

			public static class Error {
				public const string Level = "error.level";
				public const string Code = "error.code";
				public const string Message = "error.message";
				public const string StackTrace = "error.stackTrace";
				public const string MetaKeyFormat = "error.meta[{key}]";
			}
		}
	}
}
