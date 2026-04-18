package main

import "github.com/charmbracelet/lipgloss"

var (
	colAccent = lipgloss.Color("#7DD3FC")
	colMuted  = lipgloss.Color("240")
	colErr    = lipgloss.Color("203")
	colOk     = lipgloss.Color("42")
	colWarn   = lipgloss.Color("214")
	colBuy    = lipgloss.Color("42")
	colSell   = lipgloss.Color("203")
	colBg     = lipgloss.Color("235")
	colFg     = lipgloss.Color("252")

	styleTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colAccent)

	styleHeader = lipgloss.NewStyle().
			Bold(true).
			Foreground(colAccent).
			Padding(0, 1)

	styleChipOn = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("230")).
			Background(colAccent).
			Padding(0, 1)

	styleChipOff = lipgloss.NewStyle().
			Foreground(colMuted).
			Padding(0, 1)

	styleBtnBuy = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("230")).
			Background(colBuy).
			Padding(0, 2)

	styleBtnSell = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("230")).
			Background(colSell).
			Padding(0, 2)

	styleBtnNeutral = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("230")).
			Background(colAccent).
			Padding(0, 2)

	styleBtnGhost = lipgloss.NewStyle().
			Foreground(colAccent).
			Padding(0, 1)

	styleBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colMuted).
			Padding(0, 1)

	styleBoxActive = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colAccent).
			Padding(0, 1)

	styleLabel = lipgloss.NewStyle().Foreground(colMuted)
	styleKey   = lipgloss.NewStyle().Foreground(colAccent).Bold(true)

	styleOK   = lipgloss.NewStyle().Foreground(colOk)
	styleErr  = lipgloss.NewStyle().Foreground(colErr)
	styleWarn = lipgloss.NewStyle().Foreground(colWarn)
	styleBuy  = lipgloss.NewStyle().Foreground(colBuy)
	styleSell = lipgloss.NewStyle().Foreground(colSell)
	styleDim  = lipgloss.NewStyle().Foreground(colMuted)

	styleModalBg = lipgloss.NewStyle().
			Border(lipgloss.DoubleBorder()).
			BorderForeground(colAccent).
			Padding(1, 2).
			Background(colBg).
			Foreground(colFg)
)
