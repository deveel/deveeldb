using System;
using System.Linq;

namespace Deveel.Data.Design {
	public interface IMappingContext {
		void OnBuildMap(MapModelBuilder builder);
	}
}
