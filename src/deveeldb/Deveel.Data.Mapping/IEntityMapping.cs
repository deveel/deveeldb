using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Deveel.Data.Mapping {
	public interface IEntityMapping {
		Type ElementType { get; }

		Type EntityType { get; }

		string TableName { get; }
	}
}
