using System;
using System.Linq;

namespace Deveel.Data.Mapping {
	public interface IMappingContext {
		void OnBuildMap(MapModelBuilder builder);
	}
}
