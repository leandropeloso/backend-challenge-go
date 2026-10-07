package money

import (
	"errors"
	"strings"
)

var ErrInvalidCurrency = errors.New("money: invalid currency")

// Currency é um código ISO 4217 alfabético (ex.: BRL).
type Currency string

const BRL Currency = "BRL"

var iso4217 = map[string]struct{}{}

func init() {
	codes := `AED AFN ALL AMD ANG AOA ARS AUD AWG AZN BAM BBD BDT BGN BHD BIF BMD BND BOB BRL BSD BTN BWP BYN BZD
CAD CDF CHF CLP CNY COP CRC CUP CVE CZK DJF DKK DOP DZD EGP ERN ETB EUR FJD FKP GBP GEL GHS GIP GMD GNF GTQ GYD
HKD HNL HTG HUF IDR ILS INR IQD IRR ISK JMD JOD JPY KES KGS KHR KMF KPW KRW KWD KYD KZT LAK LBP LKR LRD LSL LYD
MAD MDL MGA MKD MMK MNT MOP MRU MUR MVR MWK MXN MYR MZN NAD NGN NIO NOK NPR NZD OMR PAB PEN PGK PHP PKR PLN PYG
QAR RON RSD RUB RWF SAR SBD SCR SDG SEK SGD SHP SLE SOS SRD SSP STN SVC SYP SZL THB TJS TMT TND TOP TRY TTD TWD
TZS UAH UGX USD UYU UZS VES VND VUV WST XAF XCD XOF XPF YER ZAR ZMW ZWL`
	for _, c := range strings.Fields(codes) {
		iso4217[c] = struct{}{}
	}
}

func ParseCurrency(code string) (Currency, error) {
	if _, ok := iso4217[code]; !ok {
		return "", ErrInvalidCurrency
	}
	return Currency(code), nil
}

func (c Currency) String() string { return string(c) }
