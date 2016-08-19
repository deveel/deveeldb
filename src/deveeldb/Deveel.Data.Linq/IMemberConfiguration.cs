using System;

namespace Deveel.Data.Linq {
	interface IMemberConfiguration {
		DbColumnModel CreateModel(bool isKey, KeyType keyType);
	}
}
