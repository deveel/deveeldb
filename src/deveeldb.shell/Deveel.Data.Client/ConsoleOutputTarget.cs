using System;
using System.IO;

namespace Deveel.Data.Client {
	public class ConsoleOutputTarget : TextOutputTarget {
		public ConsoleOutputTarget() 
			: base(Console.Out) {
		}

		public override bool IsInteractive {
			get { return true; }
		}

		public override void SetAttribute(OutputAttributes attributes) {
			if (attributes == OutputAttributes.None) {
				Console.ResetColor();
			} else {
				if ((attributes & OutputAttributes.BoldText) != 0) {
					Console.BackgroundColor = ConsoleColor.White;
					Console.ForegroundColor = ConsoleColor.Black;
				}

				if ((attributes & OutputAttributes.GreyText) != 0) {
					Console.ForegroundColor = ConsoleColor.Gray;
				}
			}
		}
	}
}
