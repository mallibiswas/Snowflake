-------------------------------------------------------------------
----------------- Generally useful for snowpipe'ing
-------------------------------------------------------------------

create function if not exists ZENSAND.PRESENCE.string_to_mac(A string)
    returns string
    language javascript
  as
$$
    return A.match(/.{1,2}/g).join( ':' );
$$
;

create or replace function ZENSAND.PRESENCE.decode_client_mac_info(A array)
    returns array
    language javascript
  as
$$
  // Stolen from https://stackoverflow.com/a/57909068/893578
  function base64ToHex ( txt, sep = '' ) {
     let { val, out } = base64ToHex, v1, v2, v3, v4, result = [];
     if ( ! base64ToHex.val ) { // Populate lookup tables.
        out = base64ToHex.out = [];
        val = base64ToHex.val = Array( 255 ).fill( 0 );
        const keys = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';
        for ( let i = 0 ; i < 256 ; i++ ) out.push( ( '0' + i.toString(16) ).slice( -2 ) );
        for ( let i = 0 ; i <  65 ; i++ ) val[ keys.charCodeAt( i ) ] = i;
     }
     for ( let i = 0, len = txt.length ; i < len ; i += 4 ) {
        v1 = val[ txt.charCodeAt( i   ) ]; // Map four chars to values.
        v2 = val[ txt.charCodeAt( i+1 ) ];
        v3 = val[ txt.charCodeAt( i+2 ) ];
        v4 = val[ txt.charCodeAt( i+3 ) ];
        result.push( out[ (v1 << 2) | (v2 >> 4) ], // Split values, map to output.
                     out[ ((v2 & 15) << 4) | (v3 >> 2) ],
                     out[ ((v3 & 3) << 6) | v4 ] );
     } // After loop ended: Trim result if the last values are '='.
     if ( v4 === 64 ) result.splice( v3 === 64 ? -2 : -1 );
     return result.join( sep ); // Array is fast.  String append = lots of copying.
  }
  return A.map(function(info) {
    clientMac = info.client_mac && base64ToHex(info.client_mac).toUpperCase()
    vendorPrefix = info.vendor_prefix && base64ToHex(info.vendor_prefix).toUpperCase()
    clientMacAnonymization = info.client_mac_anonymization && base64ToHex(info.client_mac_anonymization).toUpperCase()
    macPrefix = info.vendor_info && info.vendor_info.mac_prefix && base64ToHex(info.vendor_info.mac_prefix).toUpperCase()
    vendorName = info.vendor_info && info.vendor_info.vendor_name
    nonHuman = info.vendor_info && info.vendor_info.non_human
    return {
      client_mac: clientMac,
      vendor_prefix: vendorPrefix,
      vendor_info: {
        mac_prefix: macPrefix,
        vendor_name: vendorName,
        non_human: nonHuman,
      },
      client_mac_anonymization: clientMacAnonymization,
    }
  });
$$
;

create or replace file format ZENSAND.PRESENCE.S3_PARQUET_FORMAT
  type = 'PARQUET'
  BINARY_AS_TEXT = false;
