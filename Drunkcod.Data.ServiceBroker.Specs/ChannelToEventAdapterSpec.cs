using System;
using Cone;

namespace Drunkcod.Data.ServiceBroker.Specs
{
	[Describe(typeof(ChannelToEventSourceAdapter<>))]
	public class ChannelToEventSourceAdapterSpec
	{
		IChannel<string> theChannel;
		ChannelToEventSourceAdapter<string> events;

		[BeforeEach]
		public void CreateChannelAndAdapter() {
			theChannel = new BlockingCollectionChannel<string>();
			events = new ChannelToEventSourceAdapter<string>(theChannel);
		}

		public void can_pump_events_when_noone_listens() {
			theChannel.Send("");
			Check.That(() =>events.Pump(TimeSpan.Zero));
		}

		public void raises_event_when_message_received() {
			var messageReceived = false;
			var theMessage = "Helló World!";

			events.OnMessage += (s, e) => {
				messageReceived = true;
				Check.That(
					() => Object.ReferenceEquals(s, events),
					() => e.Message == theMessage);
			};
			theChannel.Send(theMessage);
			Check.That(
				() => events.Pump(TimeSpan.Zero),
				() => messageReceived);
		}

		public void doesnt_break_handler_chain_on_exception() {
			var eventsRaised = 0;
			events.OnMessage += (s, e) => ++eventsRaised;
			events.OnMessage += (s, e) => { throw new InvalidOperationException(); };
			events.OnMessage += (s, e) => ++eventsRaised;

			theChannel.Send("");
			events.Pump(TimeSpan.Zero);
			Check.That(() => eventsRaised == 2);
		}
	}
}
