use datafusion::execution::config::SessionConfig;
use std::sync::Arc;

/// An event which occurs during distributed planning (which may occur at planning time or execution
/// time if using adaptive query execution).
pub trait Event: Send + Sync + 'static {
    /// Data contained in the event.
    type Data<'a>: Clone;
    /// Result returned when a handler accepts the event.
    type Response;
}

/// Handles [`Event`] data.
pub trait EventHandler<E: Event>: Send + Sync + 'static {
    /// Returns `None` when this handler does not accept the event, allowing the next handler in
    /// the chain to try. Returns `Some` to stop dispatch and select that response.
    fn handle(&self, ev: E::Data<'_>) -> Option<E::Response>;
}

impl<E, H> EventHandler<E> for Arc<H>
where
    E: Event,
    H: EventHandler<E> + ?Sized,
{
    fn handle(&self, ev: E::Data<'_>) -> Option<E::Response> {
        self.as_ref().handle(ev)
    }
}

pub(crate) struct EventHandlerChain<E: Event> {
    pub(crate) builtin: Vec<Arc<dyn EventHandler<E>>>,
    pub(crate) custom: Vec<Arc<dyn EventHandler<E>>>,
}

impl<E: Event> Default for EventHandlerChain<E> {
    fn default() -> Self {
        Self {
            builtin: Vec::new(),
            custom: Vec::new(),
        }
    }
}

impl<E: Event> Clone for EventHandlerChain<E> {
    fn clone(&self) -> Self {
        Self {
            builtin: self.builtin.clone(),
            custom: self.custom.clone(),
        }
    }
}

impl<E: Event> EventHandlerChain<E> {
    pub(crate) fn handle(&self, ev: E::Data<'_>) -> Option<E::Response> {
        // Give priority to custom handlers registered by users.
        if let Some(res) = self
            .custom
            .iter()
            .find_map(|handler| handler.handle(ev.clone()))
        {
            return Some(res);
        }
        // If no user handler handled the event, use the built ins.
        self.builtin
            .iter()
            .find_map(|handler| handler.handle(ev.clone()))
    }

    pub(crate) fn push_builtin(cfg: &mut SessionConfig, handler: Arc<dyn EventHandler<E>>) {
        let mut handlers = cfg
            .get_extension::<Self>()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default();
        handlers.builtin.push(handler);
        cfg.set_extension(Arc::new(handlers));
    }

    pub(crate) fn push_custom(cfg: &mut SessionConfig, handler: Arc<dyn EventHandler<E>>) {
        let mut handlers = cfg
            .get_extension::<Self>()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default();
        handlers.custom.push(handler);
        cfg.set_extension(Arc::new(handlers));
    }
}
