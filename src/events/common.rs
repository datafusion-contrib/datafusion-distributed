use datafusion::execution::config::SessionConfig;
use std::sync::Arc;

pub(crate) struct EventHandlerChain<H: ?Sized> {
    builtin: Vec<Arc<H>>,
    custom: Vec<Arc<H>>,
}

impl<H: ?Sized> Default for EventHandlerChain<H> {
    fn default() -> Self {
        Self {
            builtin: Vec::new(),
            custom: Vec::new(),
        }
    }
}

impl<H: ?Sized> Clone for EventHandlerChain<H> {
    fn clone(&self) -> Self {
        Self {
            builtin: self.builtin.clone(),
            custom: self.custom.clone(),
        }
    }
}

impl<H: ?Sized> EventHandlerChain<H> {
    pub(super) fn iter(&self) -> impl Iterator<Item = &H> {
        self.custom
            .iter()
            .rev()
            .chain(&self.builtin)
            .map(AsRef::as_ref)
    }

    pub(super) fn find_map<T>(&self, mut f: impl FnMut(&H) -> Option<T>) -> Option<T> {
        // Give priority to custom handlers registered by users.
        if let Some(res) = self.custom.iter().find_map(|handler| f(handler.as_ref())) {
            return Some(res);
        }
        // If no user handler handled the event, use the built ins.
        self.builtin.iter().find_map(|handler| f(handler.as_ref()))
    }

    pub(super) fn try_fold<T, E>(
        &self,
        mut value: T,
        mut f: impl FnMut(T, &H) -> Result<T, E>,
    ) -> Result<T, E> {
        for handler in self.custom.iter().chain(&self.builtin) {
            value = f(value, handler.as_ref())?;
        }
        Ok(value)
    }
}

impl<H: ?Sized + Send + Sync + 'static> EventHandlerChain<H> {
    pub(crate) fn push_builtin(cfg: &mut SessionConfig, handler: Arc<H>) {
        let mut handlers = cfg
            .get_extension::<Self>()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default();
        handlers.builtin.push(handler);
        cfg.set_extension(Arc::new(handlers));
    }

    pub(crate) fn extend_builtin(cfg: &mut SessionConfig, handler_list: Vec<Arc<H>>) {
        let mut handlers = cfg
            .get_extension::<Self>()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default();
        for handler in handler_list {
            handlers.builtin.push(handler);
        }
        cfg.set_extension(Arc::new(handlers));
    }

    pub(crate) fn push_custom(cfg: &mut SessionConfig, handler: Arc<H>) {
        let mut handlers = cfg
            .get_extension::<Self>()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default();
        handlers.custom.push(handler);
        cfg.set_extension(Arc::new(handlers));
    }
}
