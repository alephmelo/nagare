---
trigger: always_on
---

# Coding Rules & Guidelines

- **Test-Driven Development (TDD)**: Write simple, focused tests before or alongside implementation. Ensure test coverage for core logic (DAG parsing, scheduling, execution).
- **Simplicity First**: Write readable, straightforward Go code. Avoid over-engineering, complex abstractions, or unnecessary design patterns.
- **No BS Code**: Every line should serve a purpose. If a piece of code doesn't directly contribute to the MVP goals, it shouldn't exist.
- **Meaningful Comments Only**: Do not write random or redundant comments (e.g., `// parse string` above `parseString()`). Code should be self-documenting. Only comment non-obvious "why" decisions. No monologues.
- **Maximize Reuse**: Reuse standard library functionality and well-established, lightweight community packages (like `robfig/cron/v3`) instead of rolling custom solutions, unless building it from scratch is significantly leaner. Look for opportunities to extract reusable helper functions in the project.
- **Idiomatic Go**: Use native Go concepts effectively—interfaces for abstractions, goroutines/channels for concurrency—and adhere to standard Go formatting and error handling.
- **Frontend Stack**: Use `Next.js` (App Router) with `Mantine v7` (`@mantine/core`). 
- **Frontend Styling**: Adhere strictly to Mantine components and hooks instead of custom CSS/Tailwind utilities where possible to ensure a cohesive, accessible component system. Ensure the UI feels premium with dynamic, vibrant design, Micro-animations, and a responsive layout.
- **Frontend TDD**: Keep components lean and focused. Write simple tests for data fetching or complex state logic if necessary, but prioritize rapid UI iteration over sprawling test suites for standard components.
