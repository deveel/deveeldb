using System;

using Deveel.Data.DbModel;

namespace Deveel.Data {
	public interface IDbMetadataProvider {
		string SelectedTable { get; }

		DbSchema Schema { get; }


		void Load();

		void Close();
	}
}