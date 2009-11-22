//  
//  DeveelDbProviderFactory.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data.Common;
using System.Reflection;

namespace Deveel.Data.Client {
	public sealed class DeveelDbProviderFactory : DbProviderFactory, IServiceProvider {
		private static Type dbProviderServices;

		public override bool CanCreateDataSourceEnumerator {
			get { return false; }
		}

		private static Type GetDbProviderServices() {
			if (dbProviderServices == null) {
				dbProviderServices =
					Type.GetType(
						"System.Data.Common.DbProviderServices, System.Data.Entity, " +
						"Version=3.5.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089", false, true);
			}

			return dbProviderServices;
		}

		private static object CreateDeveelDbServicesProvider() {
			//TODO: we should find a better way to handle also the version and the public token...
			const string typeName = "Deveel.Data.Entity.DeveelDbProviderServices, DeveelDb.Entity";
			Type type = Type.GetType(typeName, false);
			if (type == null)
				return null;

			return Activator.CreateInstance(type, true);
		}

		public object GetService(Type serviceType) {
			Type dbServicesType = GetDbProviderServices();
			if (serviceType != dbServicesType)
				return null;

			object service = CreateDeveelDbServicesProvider();
			return service;
		}

		public override DbCommand CreateCommand() {
			return new DeveelDbCommand();
		}

		public override DbConnection CreateConnection() {
			return new DeveelDbConnection();
		}

		public override DbDataAdapter CreateDataAdapter() {
			return new DeveelDbDataAdapter();
		}

		public override DbParameter CreateParameter() {
			return new DeveelDbParameter();
		}
	}
}