// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0.

(() => {
  const interactiveFigures = new Set([
    "single-node-dynamic-filter.svg",
    "remote-probe-cannot-share-filter.svg",
    "remote-partitioned-join.svg",
    "remote-collect-left-join.svg",
    "remote-min-aggregate.svg",
    "remote-topk-sort.svg",
    "remote-scan-read-pipeline.svg",
    "q80-s3-read-phases.svg",
  ]);
  const figureDimensions = new Map([
    ["single-node-dynamic-filter.svg", [960, 390]],
    ["remote-probe-cannot-share-filter.svg", [960, 430]],
    ["remote-partitioned-join.svg", [960, 620]],
    ["remote-collect-left-join.svg", [960, 620]],
    ["remote-min-aggregate.svg", [960, 750]],
    ["remote-topk-sort.svg", [960, 750]],
    ["remote-scan-read-pipeline.svg", [960, 660]],
    ["q80-s3-read-phases.svg", [960, 400]],
  ]);

  function enhanceFigure(image) {
    const source = image.currentSrc || image.src;
    const filename = new URL(source, window.location.href).pathname
      .split("/")
      .pop();
    if (!interactiveFigures.has(filename)) {
      return;
    }

    const object = document.createElement("object");
    object.type = "image/svg+xml";
    object.data = source;
    object.className = `${image.className} interactive-dynamic-filtering-figure`.trim();
    const [figureWidth, figureHeight] = figureDimensions.get(filename);
    const presentationFigure = image.closest(
      "section[data-class~='figure']",
    );
    if (presentationFigure) {
      object.width = figureWidth;
      object.height = figureHeight;
    } else {
      object.style.width = image.style.width || "100%";
      object.style.height = "auto";
      object.style.aspectRatio = `${figureWidth} / ${figureHeight}`;
    }
    object.style.display = "block";
    object.tabIndex = 0;
    object.setAttribute("role", "button");
    object.setAttribute("aria-pressed", "false");
    object.setAttribute(
      "aria-label",
      `${image.alt || "Animated figure"}. Click to pause or resume.`,
    );
    object.title = "Click to pause animation";
    object.textContent = image.alt || "Animated figure";

    let paused = false;
    let svgRoot;

    const toggle = () => {
      if (!svgRoot) {
        return;
      }
      paused = !paused;
      svgRoot.classList.toggle("animation-paused", paused);
      svgRoot.ownerDocument.getAnimations().forEach((animation) => {
        if (paused) {
          animation.pause();
        } else {
          animation.play();
        }
      });
      object.setAttribute("aria-pressed", String(paused));
      object.title = paused
        ? "Click to resume animation"
        : "Click to pause animation";
    };

    object.addEventListener("load", () => {
      const svgDocument = object.contentDocument;
      svgRoot = svgDocument?.documentElement;
      if (!svgRoot || !svgDocument) {
        return;
      }

      const pauseStyle = svgDocument.createElementNS(
        "http://www.w3.org/2000/svg",
        "style",
      );
      pauseStyle.textContent =
        ".animation-paused * { animation-play-state: paused !important; }";
      svgRoot.prepend(pauseStyle);
      svgRoot.style.cursor = "pointer";
      svgRoot.addEventListener("click", toggle);

      const progress = svgDocument.querySelector(
        '[aria-label="Animation progress"]',
      );
      const progressTrack = progress?.querySelector(".progress-track");
      if (progress && progressTrack) {
        progress.style.cursor = "pointer";
        progress.setAttribute("role", "slider");
        progress.setAttribute("aria-valuemin", "0");
        progress.setAttribute("aria-valuemax", "100");
        progress.setAttribute("aria-valuenow", "0");

        progress.addEventListener("click", (event) => {
          event.stopPropagation();
          const bounds = progressTrack.getBoundingClientRect();
          const fraction = Math.min(
            1,
            Math.max(0, (event.clientX - bounds.left) / bounds.width),
          );

          svgDocument.getAnimations().forEach((animation) => {
            const duration = Number(animation.effect?.getTiming().duration);
            if (Number.isFinite(duration) && duration > 0) {
              animation.currentTime = fraction * duration;
            }
          });
          progress.setAttribute(
            "aria-valuenow",
            String(Math.round(fraction * 100)),
          );
        });
      }
    });

    object.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        toggle();
      }
    });

    const imageLink = image.closest("a.image-reference");
    (imageLink || image).replaceWith(object);
  }

  function enhanceFigures() {
    document
      .querySelectorAll(
        "figure img[src$='.svg'], section[data-class~='figure'] img[src$='.svg']",
      )
      .forEach(enhanceFigure);

    document
      .querySelectorAll("section[data-class~='takeaway-reveal']")
      .forEach((slide) => {
        if (slide.dataset.revealReady === "true") {
          return;
        }
        slide.dataset.revealReady = "true";

        const bullets = Array.from(slide.querySelectorAll("ol > li"));
        let state = 0;

        const render = () => {
          bullets.forEach((bullet, index) => {
            const revealed = index < state;
            bullet.classList.toggle("is-revealed", revealed);
            bullet.setAttribute("aria-hidden", String(!revealed));
          });
          slide.dataset.revealState = String(state);
        };

        const handleArrowKey = (event) => {
          if (
            !slide.closest("svg.bespoke-marp-active") ||
            event.altKey ||
            event.ctrlKey ||
            event.metaKey ||
            event.shiftKey
          ) {
            return;
          }

          if (event.key === "ArrowRight" && state < bullets.length) {
            event.preventDefault();
            event.stopImmediatePropagation();
            state += 1;
            render();
          } else if (event.key === "ArrowLeft" && state > 0) {
            event.preventDefault();
            event.stopImmediatePropagation();
            state -= 1;
            render();
          }
        };

        document.addEventListener("keydown", handleArrowKey, true);
        render();
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", enhanceFigures);
  } else {
    enhanceFigures();
  }
})();
