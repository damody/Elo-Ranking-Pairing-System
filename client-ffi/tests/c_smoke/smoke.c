#include "erps_client.h"
#include <assert.h>
int main(void) {
  assert(erps_abi_version() == 1);
  ErpsEvent *event = 0;
  assert(erps_client_poll(0, &event) == ERPS_INVALID_ARGUMENT);
  erps_event_release(event);
  return 0;
}
