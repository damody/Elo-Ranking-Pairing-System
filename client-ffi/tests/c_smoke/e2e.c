#include "erps_client.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
typedef struct {
  ErpsClient *client;
  const char *party_id;
  uint64_t revision;
} CancelContext;
#ifdef _WIN32
#include <windows.h>
#define PAUSE() Sleep(10)
typedef HANDLE test_thread;
static DWORD WINAPI poll_other_thread(void *p) {
  ErpsEvent *e = 0;
  return (DWORD)erps_client_poll((ErpsClient *)p, &e);
}
static DWORD WINAPI state_thread(void *p) {
  return (DWORD)erps_client_get_state((ErpsClient *)p);
}
static DWORD WINAPI cancel_thread(void *p) {
  CancelContext *c = (CancelContext *)p;
  return (DWORD)erps_client_cancel_queue(c->client, c->party_id, c->revision);
}
#else
#include <pthread.h>
#include <unistd.h>
#define PAUSE() usleep(10000)
typedef pthread_t test_thread;
static void *poll_other_thread(void *p) {
  ErpsEvent *e = 0;
  return (void *)(intptr_t)erps_client_poll((ErpsClient *)p, &e);
}
static void *state_thread(void *p) {
  return (void *)(intptr_t)erps_client_get_state((ErpsClient *)p);
}
static void *cancel_thread(void *p) {
  CancelContext *c = (CancelContext *)p;
  return (void *)(intptr_t)erps_client_cancel_queue(c->client, c->party_id,
                                                    c->revision);
}
#endif
static ErpsEvent *wait_kind(ErpsClient *client, uint32_t kind) {
  for (int i = 0; i < 500; i++) {
    ErpsEvent *e = 0;
    int rc = erps_client_poll(client, &e);
    if (rc == ERPS_OK) {
      if (erps_event_kind(e) == kind)
        return e;
      erps_event_release(e);
    } else
      assert(rc == ERPS_NO_EVENT);
    PAUSE();
  }
  return 0;
}
int main(int argc, char **argv) {
  assert(argc == 2);
  ErpsClient *a = 0, *b = 0, *c = 0;
  assert(erps_client_create(argv[1], "c-player-a", &a) == ERPS_OK);
  assert(erps_client_create(argv[1], "c-player-b", &b) == ERPS_OK);
  assert(erps_client_create(argv[1], "c-player-c", &c) == ERPS_OK);
  assert(erps_client_start_events(a) == ERPS_OK);
  assert(erps_client_start_events(a) == ERPS_INVALID_ARGUMENT);
  assert(erps_client_start_events(b) == ERPS_OK);
  assert(erps_client_start_events(c) == ERPS_OK);
  for (int i = 0; i < 20; i++)
    PAUSE();
  assert(erps_client_create_party(a, "CParty1") == ERPS_OK);
  assert(erps_client_create_party(b, "CParty2") == ERPS_OK);
  ErpsEvent *pa = wait_kind(a, 1), *pb = wait_kind(b, 1);
  assert(pa && pb);
  char aid[64], bid[64];
  strcpy(aid, erps_event_entity_id(pa));
  strcpy(bid, erps_event_entity_id(pb));
  uint64_t ar = erps_event_revision(pa), br = erps_event_revision(pb);
  erps_event_release(pa);
  erps_event_release(pb);
  char invite[128];
  assert(erps_client_create_invite(a, aid, ar, 60, 1, invite, sizeof(invite)) ==
         ERPS_OK);
  assert(strlen(invite) > 0);
  assert(erps_client_join_party(c, invite) == ERPS_OK);
  ErpsEvent *pc = wait_kind(c, 1), *aj = wait_kind(a, 1);
  assert(pc && aj);
  uint64_t cr = erps_event_revision(pc);
  assert(strcmp(erps_event_party_id(pc), aid) == 0);
  erps_event_release(pc);
  erps_event_release(aj);
  assert(erps_client_leave_party(c, aid, cr) == ERPS_OK);
  ErpsEvent *al = wait_kind(a, 1);
  assert(al);
  ar = erps_event_revision(al);
  erps_event_release(al);
  assert(erps_client_rename_party(a, aid, ar, "CPartyRenamed1") == ERPS_OK);
  ErpsEvent *arenamed = wait_kind(a, 1);
  assert(arenamed);
  ar = erps_event_revision(arenamed);
  erps_event_release(arenamed);
  assert(erps_client_create_party(c, "CancelParty3") == ERPS_OK);
  ErpsEvent *cp = wait_kind(c, 1);
  assert(cp);
  char cid[64];
  strcpy(cid, erps_event_entity_id(cp));
  cr = erps_event_revision(cp);
  erps_event_release(cp);
  assert(erps_client_enqueue(c, cid, cr, ERPS_MODE_1V1, "tw") == ERPS_OK);
  ErpsEvent *cq = wait_kind(c, 1);
  assert(cq);
  cr = erps_event_revision(cq);
  erps_event_release(cq);
  CancelContext cancel_ctx = {c, cid, cr};
#ifdef _WIN32
  test_thread s1 = CreateThread(0, 0, state_thread, a, 0, 0),
              s2 = CreateThread(0, 0, state_thread, a, 0, 0);
  DWORD sr1 = 0, sr2 = 0;
  WaitForSingleObject(s1, INFINITE);
  WaitForSingleObject(s2, INFINITE);
  GetExitCodeThread(s1, &sr1);
  GetExitCodeThread(s2, &sr2);
  CloseHandle(s1);
  CloseHandle(s2);
  assert((int32_t)sr1 == ERPS_OK && (int32_t)sr2 == ERPS_OK);
  test_thread pt = CreateThread(0, 0, poll_other_thread, a, 0, 0);
  DWORD pr = 0;
  WaitForSingleObject(pt, INFINITE);
  GetExitCodeThread(pt, &pr);
  CloseHandle(pt);
  assert((int32_t)pr == ERPS_THREAD_MISUSE);
  test_thread ct = CreateThread(0, 0, cancel_thread, &cancel_ctx, 0, 0),
              cs = CreateThread(0, 0, state_thread, c, 0, 0);
  DWORD ctr = 0, csr = 0;
  WaitForSingleObject(ct, INFINITE);
  WaitForSingleObject(cs, INFINITE);
  GetExitCodeThread(ct, &ctr);
  GetExitCodeThread(cs, &csr);
  CloseHandle(ct);
  CloseHandle(cs);
  assert((int32_t)ctr == ERPS_OK && (int32_t)csr == ERPS_OK);
#else
  test_thread s1, s2, pt;
  void *sr1 = 0, *sr2 = 0, *pr = 0;
  assert(pthread_create(&s1, 0, state_thread, a) == 0);
  assert(pthread_create(&s2, 0, state_thread, a) == 0);
  pthread_join(s1, &sr1);
  pthread_join(s2, &sr2);
  assert((intptr_t)sr1 == ERPS_OK && (intptr_t)sr2 == ERPS_OK);
  assert(pthread_create(&pt, 0, poll_other_thread, a) == 0);
  pthread_join(pt, &pr);
  assert((intptr_t)pr == ERPS_THREAD_MISUSE);
  test_thread ct, cs;
  void *ctr = 0, *csr = 0;
  assert(pthread_create(&ct, 0, cancel_thread, &cancel_ctx) == 0);
  assert(pthread_create(&cs, 0, state_thread, c) == 0);
  pthread_join(ct, &ctr);
  pthread_join(cs, &csr);
  assert((intptr_t)ctr == ERPS_OK && (intptr_t)csr == ERPS_OK);
#endif
  assert(erps_client_get_state(c) == ERPS_OK);
  ErpsEvent *cstate1 = wait_kind(c, 5), *cstate2 = wait_kind(c, 5);
  assert(cstate1 && cstate2);
  assert(strlen(erps_event_ticket_id(cstate2)) == 0);
  erps_event_release(cstate1);
  erps_event_release(cstate2);
  ErpsEvent *state1 = wait_kind(a, 5), *state2 = wait_kind(a, 5);
  assert(state1 && state2);
  assert(strcmp(erps_event_party_id(state1), aid) == 0);
  erps_event_release(state1);
  erps_event_release(state2);
  assert(erps_client_enqueue(a, aid, ar, ERPS_MODE_1V1, "tw") == ERPS_OK);
  assert(erps_client_enqueue(b, bid, br, ERPS_MODE_1V1, "tw") == ERPS_OK);
  ErpsEvent *qa = wait_kind(a, 2), *qb = wait_kind(b, 2);
  assert(qa && qb);
  char proposal[64];
  strcpy(proposal, erps_event_entity_id(qa));
  assert(strcmp(proposal, erps_event_entity_id(qb)) == 0);
  erps_event_release(qa);
  erps_event_release(qb);
  int accept_a = erps_client_accept(a, proposal),
      accept_b = erps_client_accept(b, proposal);
  if (accept_a != ERPS_OK || accept_b != ERPS_OK) {
    fprintf(stderr, "accept failed: a=%d (%s) b=%d (%s)\n", accept_a,
            erps_client_last_error(a), accept_b, erps_client_last_error(b));
    return 2;
  }
  ErpsEvent *ma = wait_kind(a, 3), *mb = wait_kind(b, 3);
  assert(ma && mb);
  assert(strlen(erps_event_endpoint(ma)) > 0);
  assert(strlen(erps_event_connection_token(ma)) > 0);
  assert(strcmp(erps_event_match_id(ma), erps_event_entity_id(ma)) == 0);
  assert(erps_event_team_count(ma) == 2);
  assert(erps_event_team_player_count(ma, 0) == 1);
  assert(erps_event_team_player_count(ma, 1) == 1);
  assert(strlen(erps_event_team_player_id(ma, 0, 0)) > 0);
  assert(erps_event_team_player_id(ma, 2, 0) == 0);
  erps_event_release(ma);
  erps_event_release(mb);
  assert(erps_client_shutdown(a) == ERPS_OK);
  assert(erps_client_shutdown(b) == ERPS_OK);
  assert(erps_client_shutdown(c) == ERPS_OK);
  erps_client_destroy(a);
  erps_client_destroy(b);
  erps_client_destroy(c);
  puts("C_E2E_PASS");
  return 0;
}
