// Package forwarding defines the private Gateway-to-Engine transport contract.
package forwarding

const Version = 8

// EnvelopeBytes leaves room for the private wrapper around a public message.
const EnvelopeBytes = 4096

// NotStartedTrailer is set only when Engine proves no backend work was started.
// Its absence leaves undelivered mutation outcomes unknown.
const NotStartedTrailer = "sink-forward-not-started"
