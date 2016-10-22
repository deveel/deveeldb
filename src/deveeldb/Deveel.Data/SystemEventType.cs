using System;

namespace Deveel.Data {
	public enum SystemEventType {
		SystemCreate = 1,
		DatabaseCreate = 2,
		TableCompositeCreate = 3,
		TableCompositeSetup = 4,
		DatabaseCreated = 5,
		SystemDispose = 6
	}
}
