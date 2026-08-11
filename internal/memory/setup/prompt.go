package setup

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/mattn/go-isatty"
	"golang.org/x/term"
)

// ANSI styling; cleared to empty if stdout is not a TTY.
var (
	colorGreen = "\033[32m"
	colorDim   = "\033[2m"
	colorRed   = "\033[31m"
	colorBold  = "\033[1m"
	colorReset = "\033[0m"

	symOK   = "✓"
	symFail = "✗"
	symDot  = "·"
)

var colorOnce sync.Once

func initColors() {
	colorOnce.Do(func() {
		if !(isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())) {
			colorGreen = ""
			colorDim = ""
			colorRed = ""
			colorBold = ""
			colorReset = ""
		}
	})
}

// IsInteractive returns true if stdin is a TTY.
func IsInteractive() bool {
	return isatty.IsTerminal(os.Stdin.Fd()) || isatty.IsCygwinTerminal(os.Stdin.Fd())
}

// Confirm prompts the user for a yes/no answer.
func Confirm(prompt string, defaultYes bool) bool {
	initColors()
	hint := "y/N"
	if defaultYes {
		hint = "Y/n"
	}
	fmt.Printf("%s %s[%s]%s › ", prompt, colorDim, hint, colorReset)

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return defaultYes
	}
	answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
	if answer == "" {
		return defaultYes
	}
	return answer == "y" || answer == "yes"
}

// SelectMulti shows a multi-select prompt with arrow key navigation.
// Uses raw terminal mode for ↑↓ + space selection when possible,
// falls back to number-input toggle for non-TTY environments.
func SelectMulti(title string, options []string, defaults []bool) []bool {
	initColors()
	if !IsInteractive() {
		return selectMultiFallback(title, options, defaults)
	}

	fd := int(os.Stdin.Fd())
	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return selectMultiFallback(title, options, defaults)
	}
	defer term.Restore(fd, oldState)

	selected := make([]bool, len(options))
	copy(selected, defaults)
	cursor := 0

	// Number of lines we render (header + options + hint)
	renderLines := len(options) + 2

	render := func(first bool) {
		if !first {
			// Move cursor up to overwrite previous render
			fmt.Printf("\033[%dA", renderLines)
		}
		// Header
		fmt.Printf("\033[2K  %s%s%s %s(↑↓ move, space toggle, enter confirm)%s\r\n",
			colorBold, title, colorReset, colorDim, colorReset)
		// Options
		for i, opt := range options {
			fmt.Print("\033[2K") // clear line
			pointer := "    "
			if i == cursor {
				pointer = fmt.Sprintf("  %s›%s ", colorGreen, colorReset)
			}
			if selected[i] {
				fmt.Printf("%s%s[x]%s %s\r\n", pointer, colorGreen, colorReset, opt)
			} else {
				fmt.Printf("%s%s[ ]%s %s%s%s\r\n", pointer, colorDim, colorReset, colorDim, opt, colorReset)
			}
		}
		// Hint line
		fmt.Print("\033[2K")
		count := 0
		for _, s := range selected {
			if s {
				count++
			}
		}
		if count > 0 {
			fmt.Printf("  %s%d selected%s\r\n", colorDim, count, colorReset)
		} else {
			fmt.Printf("  %sNone selected%s\r\n", colorDim, colorReset)
		}
	}

	render(true)

	buf := make([]byte, 3)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil || n == 0 {
			break
		}

		switch {
		// Enter
		case buf[0] == '\r' || buf[0] == '\n':
			// Erase the interactive UI and print compact summary
			fmt.Printf("\033[%dA", renderLines)
			for i := 0; i < renderLines; i++ {
				fmt.Print("\033[2K\r\n")
			}
			fmt.Printf("\033[%dA", renderLines)

			// Print final summary
			var names []string
			for i, opt := range options {
				if selected[i] {
					names = append(names, opt)
				}
			}
			if len(names) > 0 {
				fmt.Printf("  %s%s%s Selected: %s\r\n", colorGreen, symOK, colorReset, strings.Join(names, ", "))
			} else {
				fmt.Printf("  %s%s None selected%s\r\n", colorDim, symDot, colorReset)
			}
			// Clear remaining lines from old render
			for i := 1; i < renderLines; i++ {
				fmt.Print("\033[2K\r\n")
			}
			return selected

		// Escape or q — cancel (return defaults)
		case buf[0] == 0x1b && n == 1:
			return defaults
		case buf[0] == 'q':
			return defaults

		// Space — toggle
		case buf[0] == ' ':
			selected[cursor] = !selected[cursor]
			render(false)

		// Arrow keys: ESC [ A/B
		case n >= 3 && buf[0] == 0x1b && buf[1] == '[':
			switch buf[2] {
			case 'A': // up
				if cursor > 0 {
					cursor--
				}
				render(false)
			case 'B': // down
				if cursor < len(options)-1 {
					cursor++
				}
				render(false)
			}

		// j/k vim keys
		case buf[0] == 'j':
			if cursor < len(options)-1 {
				cursor++
			}
			render(false)
		case buf[0] == 'k':
			if cursor > 0 {
				cursor--
			}
			render(false)

		// Ctrl-C — abort
		case buf[0] == 3:
			return defaults
		}
	}

	return selected
}

// selectMultiFallback is the non-interactive number-input fallback.
func selectMultiFallback(title string, options []string, defaults []bool) []bool {
	selected := make([]bool, len(options))
	copy(selected, defaults)

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("\n%s%s%s %s(toggle: 1-%d, confirm: Enter)%s\n",
			colorBold, title, colorReset, colorDim, len(options), colorReset)
		for i, opt := range options {
			if selected[i] {
				fmt.Printf("  %d. %s[x]%s %s\n", i+1, colorGreen, colorReset, opt)
			} else {
				fmt.Printf("  %d. %s[ ]%s %s%s%s\n", i+1, colorDim, colorReset, colorDim, opt, colorReset)
			}
		}
		fmt.Print("› ")

		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			break
		}

		num, err := strconv.Atoi(input)
		if err != nil || num < 1 || num > len(options) {
			fmt.Printf("  %s%s invalid: %s%s\n", colorRed, symFail, input, colorReset)
			continue
		}
		selected[num-1] = !selected[num-1]
	}

	return selected
}

// SelectOne shows a single-select prompt with arrow key navigation.
// Returns the index of the selected option.
func SelectOne(prompt string, options []string, defaultIdx int) int {
	initColors()
	if !IsInteractive() || len(options) == 0 {
		return defaultIdx
	}

	fd := int(os.Stdin.Fd())
	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return defaultIdx
	}
	defer term.Restore(fd, oldState)

	cursor := defaultIdx
	if cursor < 0 || cursor >= len(options) {
		cursor = 0
	}

	renderLines := len(options) + 1 // header + options

	render := func(first bool) {
		if !first {
			fmt.Printf("\033[%dA", renderLines)
		}
		fmt.Printf("\033[2K  %s%s%s %s(↑↓ move, enter confirm)%s\r\n",
			colorBold, prompt, colorReset, colorDim, colorReset)
		for i, opt := range options {
			fmt.Print("\033[2K")
			if i == cursor {
				fmt.Printf("  %s›%s %s\r\n", colorGreen, colorReset, opt)
			} else {
				fmt.Printf("    %s%s%s\r\n", colorDim, opt, colorReset)
			}
		}
	}

	render(true)

	buf := make([]byte, 3)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil || n == 0 {
			break
		}

		switch {
		case buf[0] == '\r' || buf[0] == '\n':
			// Erase UI and print summary
			fmt.Printf("\033[%dA", renderLines)
			for i := 0; i < renderLines; i++ {
				fmt.Print("\033[2K\r\n")
			}
			fmt.Printf("\033[%dA", renderLines)
			fmt.Printf("  %s%s%s %s: %s\r\n", colorGreen, symOK, colorReset, prompt, options[cursor])
			for i := 1; i < renderLines; i++ {
				fmt.Print("\033[2K\r\n")
			}
			return cursor

		case buf[0] == 0x1b && n == 1:
			return defaultIdx
		case buf[0] == 'q':
			return defaultIdx
		case buf[0] == 3:
			return defaultIdx

		case n >= 3 && buf[0] == 0x1b && buf[1] == '[':
			switch buf[2] {
			case 'A':
				if cursor > 0 {
					cursor--
				}
				render(false)
			case 'B':
				if cursor < len(options)-1 {
					cursor++
				}
				render(false)
			}

		case buf[0] == 'k':
			if cursor > 0 {
				cursor--
			}
			render(false)
		case buf[0] == 'j':
			if cursor < len(options)-1 {
				cursor++
			}
			render(false)
		}
	}

	return cursor
}

// ReadLine reads a single line of text input.
func ReadLine(prompt string) string {
	initColors()
	fmt.Printf("  %s›%s %s", colorGreen, colorReset, prompt)

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return ""
	}
	return strings.TrimSpace(scanner.Text())
}

// ReadMultiLine reads multi-line text input until a blank line or EOF.
func ReadMultiLine(prompt string) string {
	initColors()
	fmt.Printf("\n  %s%s%s\n", colorDim, prompt, colorReset)

	scanner := bufio.NewScanner(os.Stdin)
	var lines []string
	for {
		fmt.Printf("  %s›%s ", colorGreen, colorReset)
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		if line == "" {
			break
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

// PrintPreview prints text in dim color with indentation, suitable for showing defaults.
func PrintPreview(text string) {
	initColors()
	for _, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		fmt.Printf("  %s%s%s\n", colorDim, line, colorReset)
	}
}

// StatusOK prints a green checkmark status line.
func StatusOK(step, total int, label, detail string) {
	initColors()
	fmt.Printf("  %s%s%s %-12s %s%s%s\n",
		colorGreen, symOK, colorReset, label, colorDim, detail, colorReset)
}

// StatusUpdated prints a green checkmark with "updated" note.
func StatusUpdated(step, total int, label, detail string) {
	initColors()
	fmt.Printf("  %s%s%s %-12s %s%s%s  %supdated%s\n",
		colorGreen, symOK, colorReset, label, colorDim, detail, colorReset, colorGreen, colorReset)
}

// StatusSkipped prints a dimmed dot status line.
func StatusSkipped(step, total int, label, detail string) {
	initColors()
	fmt.Printf("  %s%s %-12s %s%s\n",
		colorDim, symDot, label, detail, colorReset)
}

// StatusError prints a red cross status line.
func StatusError(step, total int, label string, err error) {
	initColors()
	fmt.Printf("  %s%s%s %-12s %s%s%s\n",
		colorRed, symFail, colorReset, label, colorRed, err, colorReset)
}

// DetectionLine prints a detection result line.
func DetectionLine(detected bool, display, version, path string) {
	initColors()
	displayPath := strings.Replace(path, HomeDir(), "~", 1)
	if detected {
		ver := version
		if ver == "" {
			ver = ""
		}
		fmt.Printf("  %s%s%s %-14s %s%-12s %s%s\n",
			colorGreen, symOK, colorReset, display, colorDim, ver, displayPath, colorReset)
	} else {
		fmt.Printf("  %s%s %-14s (not found)%s\n",
			colorDim, symDot, display, colorReset)
	}
}
