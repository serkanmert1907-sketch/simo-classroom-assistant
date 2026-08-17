import { DurableObject } from 'cloudflare:workers';

const JSON_HEADERS = {
  'content-type': 'application/json; charset=utf-8',
  'cache-control': 'no-store',
  'x-content-type-options': 'nosniff',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}

function cleanRoom(value) {
  return String(value || '').replace(/[^0-9A-Za-z_-]/g, '').slice(0, 32);
}

function cleanText(value, max = 500) {
  return String(value ?? '').replace(/[<>]/g, '').trim().slice(0, max);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/api/health') {
      return json({
        ok: true,
        service: 'simo-classroom-assistant-v1',
        realtime: 'durable-object-websocket-hibernation',
        liveStorageWrites: 0,
      });
    }

    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return json({ ok: false, error: 'WebSocket upgrade gerekli' }, 426);
      }
      const room = cleanRoom(url.searchParams.get('room'));
      if (!room) return json({ ok: false, error: 'room gerekli' }, 400);
      return env.CLASSROOMS.getByName(room).fetch(request);
    }

    return env.ASSETS.fetch(request);
  },
};

export class ClassroomRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    this.roomState = null;
    this.needsTeacherSync = false;
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }

  baseState(room) {
    return {
      room,
      classTitle: 'Canlı Ders',
      currentQuestion: null,
      selectedStudentId: null,
      workspace: {
        enabled: false,
        controllerStudentId: null,
        title: 'Ortak Çalışma',
        prompt: 'Öğretmenin verdiği çalışmayı burada birlikte çözün.',
        strokes: [],
        note: '',
        revision: 0,
      },
      reactions: { understood: 0, repeat: 0, slower: 0, example: 0 },
      questionSerial: 0,
      activity: [],
      updatedAt: Date.now(),
    };
  }

  ensureState(room) {
    if (this.roomState?.room === room) return this.roomState;
    const existingSockets = this.ctx.getWebSockets();
    const base = this.baseState(room);
    if (existingSockets.length) this.needsTeacherSync = true;
    for (const ws of existingSockets) {
      const att = ws.deserializeAttachment?.() || {};
      if (att.role === 'teacher' && att.teacherState) {
        Object.assign(base, att.teacherState);
        break;
      }
    }
    this.roomState = base;
    return this.roomState;
  }

  saveTeacherState() {
    const state = this.roomState;
    if (!state) return;
    const teacherState = {
      classTitle: state.classTitle,
      currentQuestion: state.currentQuestion,
      selectedStudentId: state.selectedStudentId,
      workspace: {
        ...state.workspace,
        strokes: [],
        note: cleanText(state.workspace?.note || '', 700),
      },
      reactions: state.reactions,
      questionSerial: state.questionSerial,
      activity: state.activity.slice(-10),
      updatedAt: state.updatedAt,
    };
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment?.() || {};
      if (att.role === 'teacher') {
        ws.serializeAttachment({ ...att, teacherState });
      }
    }
  }

  listStudents() {
    const out = [];
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment?.() || {};
      if (att.role !== 'student') continue;
      out.push({
        id: att.studentId,
        name: att.name,
        joinedAt: att.joinedAt,
        lastAnswer: att.lastAnswer ?? null,
        lastAnswerCorrect: att.lastAnswerCorrect ?? null,
        reaction: att.reaction || null,
        workspaceControl: this.roomState?.workspace?.controllerStudentId === att.studentId,
      });
    }
    return out;
  }

  snapshot() {
    const state = this.roomState;
    return {
      type: 'snapshot',
      room: state.room,
      classTitle: state.classTitle,
      currentQuestion: state.currentQuestion,
      selectedStudentId: state.selectedStudentId,
      workspace: state.workspace,
      reactions: state.reactions,
      students: this.listStudents(),
      activity: state.activity.slice(-30),
      updatedAt: state.updatedAt,
    };
  }

  safeSend(ws, data) {
    try { ws.send(JSON.stringify(data)); } catch {}
  }

  broadcast(data, filter = null) {
    const payload = JSON.stringify(data);
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment?.() || {};
      if (filter && !filter(att)) continue;
      try { ws.send(payload); } catch {}
    }
  }

  addActivity(kind, text, studentId = null) {
    const item = { id: crypto.randomUUID(), at: Date.now(), kind, text: cleanText(text, 180), studentId };
    this.roomState.activity.push(item);
    if (this.roomState.activity.length > 60) this.roomState.activity.shift();
    return item;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const room = cleanRoom(url.searchParams.get('room'));
    const role = url.searchParams.get('role') === 'teacher' ? 'teacher' : 'student';
    if (!room) return json({ ok: false, error: 'room gerekli' }, 400);

    const state = this.ensureState(room);
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    const studentId = role === 'student'
      ? cleanText(url.searchParams.get('studentId') || crypto.randomUUID(), 80)
      : '';
    const name = role === 'student' ? (cleanText(url.searchParams.get('name'), 60) || 'Öğrenci') : 'Öğretmen';

    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({
      role,
      room,
      studentId,
      name,
      joinedAt: Date.now(),
      reaction: null,
      lastAnswer: null,
      lastAnswerCorrect: null,
      teacherState: role === 'teacher' ? {
        classTitle: state.classTitle,
        currentQuestion: state.currentQuestion,
        selectedStudentId: state.selectedStudentId,
        workspace: { ...state.workspace, strokes: [], note: cleanText(state.workspace?.note || '', 700) },
        reactions: state.reactions,
        questionSerial: state.questionSerial,
        activity: state.activity.slice(-10),
        updatedAt: state.updatedAt,
      } : undefined,
    });

    const activity = this.addActivity(role === 'teacher' ? 'teacher' : 'join', role === 'teacher' ? 'Öğretmen bağlandı' : `${name} derse katıldı`, studentId || null);
    this.safeSend(server, this.snapshot());
    this.broadcast({ type: 'activity', item: activity });
    this.broadcast({ type: 'presence', students: this.listStudents() });
    this.saveTeacherState();

    return new Response(null, { status: 101, webSocket: client });
  }

  parseMessage(message) {
    if (typeof message !== 'string' || message.length > 32_000) return null;
    try { return JSON.parse(message); } catch { return null; }
  }

  evaluateAnswer(question, value) {
    if (!question) return null;
    if (question.type === 'quiz' || question.type === 'truefalse') {
      return Number(value) === Number(question.correctIndex);
    }
    if (question.type === 'fill') {
      const normal = (s) => String(s ?? '').trim().toLocaleLowerCase('tr-TR').replace(/\s+/g, ' ');
      const accepted = Array.isArray(question.acceptedAnswers) ? question.acceptedAnswers : [];
      return accepted.map(normal).includes(normal(value));
    }
    return null;
  }

  updateStudentAttachment(ws, patch) {
    const att = ws.deserializeAttachment?.() || {};
    ws.serializeAttachment({ ...att, ...patch });
    return { ...att, ...patch };
  }

  async webSocketMessage(ws, message) {
    const data = this.parseMessage(message);
    if (!data) return;
    const att = ws.deserializeAttachment?.() || {};
    const state = this.ensureState(att.room || '');
    if (this.needsTeacherSync) {
      this.needsTeacherSync = false;
      this.broadcast({ type: 'sync_request' }, x => x.role === 'teacher');
    }
    state.updatedAt = Date.now();

    if (att.role === 'teacher') {
      if (data.type === 'teacher_sync') {
        if (data.workspace && typeof data.workspace === 'object') {
          state.workspace = {
            ...state.workspace,
            enabled: !!data.workspace.enabled,
            controllerStudentId: cleanText(data.workspace.controllerStudentId, 80) || null,
            title: cleanText(data.workspace.title, 100) || 'Ortak Çalışma',
            prompt: cleanText(data.workspace.prompt, 500) || 'Birlikte çözün.',
            note: cleanText(data.workspace.note, 1500),
            strokes: Array.isArray(data.workspace.strokes) ? data.workspace.strokes.slice(-220) : [],
            revision: Number(data.workspace.revision || state.workspace.revision || 0),
          };
          this.broadcast({ type: 'workspace', workspace: state.workspace });
        }
        return;
      }
      if (data.type === 'set_title') {
        state.classTitle = cleanText(data.title, 100) || 'Canlı Ders';
        this.broadcast({ type: 'class_title', title: state.classTitle });
      }

      if (data.type === 'question_open') {
        state.questionSerial += 1;
        const q = data.question || {};
        const type = ['quiz', 'fill', 'truefalse', 'short', 'poll'].includes(q.type) ? q.type : 'quiz';
        state.currentQuestion = {
          id: `${Date.now()}-${state.questionSerial}`,
          type,
          prompt: cleanText(q.prompt, 700),
          options: Array.isArray(q.options) ? q.options.slice(0, 6).map(x => cleanText(x, 160)) : [],
          correctIndex: Number.isInteger(q.correctIndex) ? q.correctIndex : null,
          acceptedAnswers: Array.isArray(q.acceptedAnswers) ? q.acceptedAnswers.slice(0, 12).map(x => cleanText(x, 120)) : [],
          reveal: false,
          openedAt: Date.now(),
        };
        for (const studentWs of this.ctx.getWebSockets()) {
          const sa = studentWs.deserializeAttachment?.() || {};
          if (sa.role !== 'student') continue;
          studentWs.serializeAttachment({ ...sa, lastAnswer: null, lastAnswerCorrect: null });
        }
        const activity = this.addActivity('question', 'Yeni soru gönderildi');
        this.broadcast({ type: 'question', question: state.currentQuestion });
        this.broadcast({ type: 'activity', item: activity });
        this.broadcast({ type: 'presence', students: this.listStudents() });
      }

      if (data.type === 'question_reveal' && state.currentQuestion) {
        state.currentQuestion.reveal = true;
        this.broadcast({ type: 'question', question: state.currentQuestion });
      }

      if (data.type === 'question_close') {
        state.currentQuestion = null;
        this.broadcast({ type: 'question', question: null });
      }

      if (data.type === 'select_student') {
        state.selectedStudentId = cleanText(data.studentId, 80) || null;
        this.broadcast({ type: 'selected_student', studentId: state.selectedStudentId });
      }

      if (data.type === 'workspace_config') {
        const enabled = !!data.enabled;
        state.workspace = {
          ...state.workspace,
          enabled,
          controllerStudentId: enabled ? (cleanText(data.controllerStudentId, 80) || null) : null,
          title: cleanText(data.title, 100) || 'Ortak Çalışma',
          prompt: cleanText(data.prompt, 500) || 'Birlikte çözün.',
          revision: Number(state.workspace.revision || 0) + 1,
        };
        if (!enabled) state.workspace.strokes = [];
        this.broadcast({ type: 'workspace', workspace: state.workspace });
        this.broadcast({ type: 'presence', students: this.listStudents() });
      }

      if (data.type === 'workspace_clear') {
        state.workspace.strokes = [];
        state.workspace.note = '';
        state.workspace.revision += 1;
        this.broadcast({ type: 'workspace', workspace: state.workspace });
      }

      if (data.type === 'workspace_stroke') {
        const stroke = data.stroke;
        if (stroke && Array.isArray(stroke.points) && stroke.points.length <= 140) {
          state.workspace.strokes.push({
            id: cleanText(stroke.id, 80) || crypto.randomUUID(),
            color: cleanText(stroke.color, 20) || '#1d4ed8',
            width: Math.max(1, Math.min(12, Number(stroke.width || 3))),
            points: stroke.points.slice(0, 140).map(p => [Number(p[0]) || 0, Number(p[1]) || 0]),
          });
          if (state.workspace.strokes.length > 220) state.workspace.strokes.shift();
          state.workspace.revision += 1;
          this.broadcast({ type: 'workspace_stroke', stroke: state.workspace.strokes.at(-1), revision: state.workspace.revision });
        }
      }

      if (data.type === 'workspace_note') {
        state.workspace.note = cleanText(data.note, 1500);
        state.workspace.revision += 1;
        this.broadcast({ type: 'workspace_note', note: state.workspace.note, revision: state.workspace.revision });
      }

      this.saveTeacherState();
      return;
    }

    if (data.type === 'answer') {
      const q = state.currentQuestion;
      if (!q) return;
      const rawValue = q.type === 'quiz' || q.type === 'truefalse' ? Number(data.value) : cleanText(data.value, 500);
      const correct = this.evaluateAnswer(q, rawValue);
      const nextAtt = this.updateStudentAttachment(ws, { lastAnswer: rawValue, lastAnswerCorrect: correct });
      const activity = this.addActivity('answer', `${nextAtt.name} cevap verdi`, nextAtt.studentId);
      this.broadcast({
        type: 'answer',
        studentId: nextAtt.studentId,
        name: nextAtt.name,
        value: rawValue,
        correct,
        questionId: q.id,
      }, x => x.role === 'teacher');
      this.broadcast({ type: 'activity', item: activity });
      this.broadcast({ type: 'presence', students: this.listStudents() });
      return;
    }

    if (data.type === 'reaction') {
      const reaction = ['understood', 'repeat', 'slower', 'example'].includes(data.reaction) ? data.reaction : null;
      const previous = att.reaction || null;
      if (previous && state.reactions[previous] > 0) state.reactions[previous] -= 1;
      if (reaction) state.reactions[reaction] += 1;
      const nextAtt = this.updateStudentAttachment(ws, { reaction });
      this.broadcast({ type: 'reactions', reactions: state.reactions });
      const activity = this.addActivity('reaction', `${nextAtt.name}: ${reaction || 'tepki kaldırıldı'}`, nextAtt.studentId);
      this.broadcast({ type: 'activity', item: activity }, x => x.role === 'teacher');
      this.saveTeacherState();
      return;
    }

    const isController = state.workspace.enabled && state.workspace.controllerStudentId === att.studentId;
    if (isController && data.type === 'workspace_stroke') {
      const stroke = data.stroke;
      if (stroke && Array.isArray(stroke.points) && stroke.points.length <= 140) {
        state.workspace.strokes.push({
          id: cleanText(stroke.id, 80) || crypto.randomUUID(),
          color: cleanText(stroke.color, 20) || '#0f766e',
          width: Math.max(1, Math.min(12, Number(stroke.width || 3))),
          points: stroke.points.slice(0, 140).map(p => [Number(p[0]) || 0, Number(p[1]) || 0]),
          by: att.studentId,
        });
        if (state.workspace.strokes.length > 220) state.workspace.strokes.shift();
        state.workspace.revision += 1;
        this.broadcast({ type: 'workspace_stroke', stroke: state.workspace.strokes.at(-1), revision: state.workspace.revision });
        this.saveTeacherState();
      }
      return;
    }

    if (isController && data.type === 'workspace_note') {
      state.workspace.note = cleanText(data.note, 1500);
      state.workspace.revision += 1;
      this.broadcast({ type: 'workspace_note', note: state.workspace.note, revision: state.workspace.revision });
      this.saveTeacherState();
    }
  }

  async webSocketClose(ws) {
    const att = ws.deserializeAttachment?.() || {};
    if (att.role === 'student') {
      const activity = this.addActivity('leave', `${att.name || 'Öğrenci'} ayrıldı`, att.studentId || null);
      this.broadcast({ type: 'activity', item: activity });
      this.broadcast({ type: 'presence', students: this.listStudents() });
      this.saveTeacherState();
    }
  }
}
