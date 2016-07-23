using System;

namespace Deveel.Data.Linq {
	interface ITypeConfiguration {
		DbTypeModel CreateModel();
	}
}
